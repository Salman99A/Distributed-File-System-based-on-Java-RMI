import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;


public class Client {


	MasterServerClientInterface masterStub;
	static Registry registry;
	int regPort = Configurations.REG_PORT;
	String regAddr = Configurations.REG_ADDR;
	int chunkSize = Configurations.CHUNK_SIZE; // in bytes 
	
	public Client() {
		try {
			registry = LocateRegistry.getRegistry(regAddr, regPort);
			masterStub =  (MasterServerClientInterface) registry.lookup("MasterServerClientInterface");
			System.out.println("[@client] Master Stub fetched successfuly");
		} catch (RemoteException | NotBoundException e) {
			// fatal error .. no registry could be linked
			e.printStackTrace();
		}
	}

	public byte[] read(String fileName) throws IOException, NotBoundException{
		List<ReplicaLoc> locations = masterStub.read(fileName);
		System.out.println("[@client] Master Granted read operation for " + fileName);
		
		IOException lastEx = null;
		for (int i = 0; i < locations.size(); i++) {
			ReplicaLoc replicaLoc = locations.get(i);
			if (!replicaLoc.isAlive()) {
				int nextReplica = (i + 1 < locations.size()) ? locations.get(i + 1).getId() : -1;
				TrafficMonitor.recordFailover(replicaLoc.getId(), nextReplica, fileName, "Node offline");
				System.out.println("[@client] [FAILOVER] Replica " + replicaLoc.getId() + " is OFFLINE! Automatically transferring read traffic to next available node...");
				continue;
			}
			try {
				ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + replicaLoc.getId());
				FileContent fileContent = replicaStub.read(fileName);
				System.out.println("[@client] read operation completed successfully from Replica " + replicaLoc.getId());
				System.out.println("[@client] data:\n" + new String(fileContent.getData()));
				return fileContent.getData();
			} catch (Exception ex) {
				replicaLoc.setAlive(false);
				int nextReplica = (i + 1 < locations.size()) ? locations.get(i + 1).getId() : -1;
				TrafficMonitor.recordFailover(replicaLoc.getId(), nextReplica, fileName, "Read error: " + ex.getMessage());
				System.out.println("[@client] [FAILOVER] Read from Replica " + replicaLoc.getId() + " failed (" + ex.getMessage() + "). Transferring traffic to surviving replica...");
				lastEx = new IOException(ex);
			}
		}
		
		if (lastEx != null) throw lastEx;
		throw new FileNotFoundException("All replica servers hosting " + fileName + " are currently offline!");
	}
	
	public ReplicaServerClientInterface initWrite(String fileName, Long txnID) throws IOException, NotBoundException{
		WriteAck ackMsg = masterStub.write(fileName);
		txnID = Long.valueOf(ackMsg.getTransactionId());
		return (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());
	}
	
	public void writeChunk (long txnID, String fileName, byte[] chunk, long seqN, ReplicaServerClientInterface replicaStub) throws RemoteException, IOException{
		
		FileContent fileContent = new FileContent(fileName, chunk);
		ChunkAck chunkAck;
		
		do { 
			chunkAck = replicaStub.write(txnID, seqN, fileContent);
		} while(chunkAck.getSeqNo() != seqN);
	}
	
	public void write (String fileName, byte[] data) throws IOException, NotBoundException, MessageNotFoundException{
		int maxRetries = 3;
		Exception lastEx = null;

		for (int attempt = 0; attempt < maxRetries; attempt++) {
			try {
				WriteAck ackMsg = masterStub.write(fileName);
				ReplicaLoc targetLoc = ackMsg.getLoc();
				int targetId = targetLoc.getId();
				ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + targetId);
				
				System.out.println("[@client] Master granted write operation to Primary Replica " + targetId);
				
				int segN = (int) Math.max(1, Math.ceil(1.0 * data.length / chunkSize));
				FileContent fileContent = new FileContent(fileName);
				ChunkAck chunkAck;
				byte[] chunk = new byte[chunkSize];
				
				for (int i = 0; i < segN - 1; i++) {
					System.arraycopy(data, i * chunkSize, chunk, 0, chunkSize);
					fileContent.setData(chunk);
					do { 
						chunkAck = replicaStub.write(ackMsg.getTransactionId(), i, fileContent);
					} while(chunkAck.getSeqNo() != i);
				}

				// Handling last chunk of the file < chunk size
				int lastChunkLen = (data.length % chunkSize > 0) ? (data.length % chunkSize) : (data.length == 0 ? 0 : chunkSize);
				chunk = new byte[lastChunkLen];
				if (data.length > 0) {
					System.arraycopy(data, (segN - 1) * chunkSize, chunk, 0, lastChunkLen);
				}
				fileContent.setData(chunk);
				do { 
					chunkAck = replicaStub.write(ackMsg.getTransactionId(), segN - 1, fileContent);
				} while(chunkAck.getSeqNo() != segN - 1);
				
				System.out.println("[@client] write operation complete on Replica " + targetId);
				replicaStub.commit(ackMsg.getTransactionId(), segN);
				System.out.println("[@client] commit operation complete on Replica " + targetId);
				return; // Successful write!

			} catch (Exception ex) {
				System.err.println("[@client] [FAILOVER] Write attempt " + (attempt + 1) + " failed: " + ex.getMessage() + ". Requesting Master failover...");
				lastEx = ex;
				try {
					Thread.sleep(300);
				} catch (InterruptedException ignored) {}
			}
		}

		if (lastEx instanceof IOException) throw (IOException) lastEx;
		if (lastEx instanceof NotBoundException) throw (NotBoundException) lastEx;
		if (lastEx instanceof MessageNotFoundException) throw (MessageNotFoundException) lastEx;
		throw new IOException("Failed to write to any replica after " + maxRetries + " attempts: " + (lastEx != null ? lastEx.getMessage() : "unknown"));
	}
	
	public void commit(String fileName, long txnID, long seqN) throws MessageNotFoundException, IOException, NotBoundException{
		ReplicaLoc primaryLoc = masterStub.locatePrimaryReplica(fileName);
		ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
		primaryStub.commit(txnID, seqN);
		System.out.println("[@client] commit operation complete");
	}
	
	public void batchOperations(String[] cmds){
		System.out.println("[@client] batch operations started");
		String cmd ;
		String[] tokens;
		for (int i = 0; i < cmds.length; i++) {
			cmd = cmds[i];
			tokens = cmd.split(", ");
			try {
				if (tokens[0].trim().equals("read"))
					this.read(tokens[1].trim());
				else if (tokens[0].trim().equals("write"))
					this.write(tokens[1].trim(), tokens[2].trim().getBytes());
				else if (tokens[0].trim().equals("commit"))
						this.commit(tokens[1].trim(), Long.parseLong(tokens[2].trim()), Long.parseLong(tokens[3].trim()));
			}catch (IOException | NotBoundException | MessageNotFoundException e){
				System.err.println("Operation "+i+" Failed");
			}
		}
		System.out.println("[@client] batch operations completed");
	}
	
}
