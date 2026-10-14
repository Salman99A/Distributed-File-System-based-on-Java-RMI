import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;


public class Main {

	public static int regPort = Configurations.REG_PORT;

	public static Registry registry ;

	/**
	 * respawns replica servers and register replicas at master
	 * @param master
	 * @throws IOException
	 */
	public static void respawnReplicaServers(Master master)throws IOException{
		System.out.println("[@main] respawning replica servers ");
		// TODO make file names global
		BufferedReader br = new BufferedReader(new FileReader("repServers.txt"));
		int n = Math.min(5, Integer.parseInt(br.readLine().trim()));
		ReplicaLoc replicaLoc;
		String s;

		for (int i = 0; i < n; i++) {
			s = br.readLine().trim();
			replicaLoc = new ReplicaLoc(i, s.substring(0, s.indexOf(':')) , true);
			ReplicaServer rs = new ReplicaServer(i, "./"); 

			ReplicaInterface stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
			registry.rebind("ReplicaClient"+i, stub);

			master.registerReplicaServer(replicaLoc, stub);

			System.out.println("replica server state [@ main] = "+rs.isAlive());
		}
		br.close();
	}

	public static void launchClients(){
		try {
			Client c = new Client();
			char[] ss = "File 1 test test END ".toCharArray();
			byte[] data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++) 
				data[i] = (byte) ss[i];

			c.write("file1", data);
			byte[] ret = c.read("file1");
			System.out.println("file1: " + new String(ret));

			c = new Client();
			ss = "File 1 Again Again END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++) 
				data[i] = (byte) ss[i];

			c.write("file1", data);
			ret = c.read("file1");
			System.out.println("file1: " + new String(ret));

			c = new Client();
			ss = "File 2 test test END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++) 
				data[i] = (byte) ss[i];

			c.write("file2", data);
			ret = c.read("file2");
			System.out.println("file2: " + new String(ret));

		} catch (NotBoundException | IOException | MessageNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * runs a custom test as follows
	 * 1. write initial text to "file1"
	 * 2. reads the recently text written to "file1"
	 * 3. writes a new message to "file1"
	 * 4. while the writing operation in progress read the content of "file1"
	 * 5. the read content should be = to the initial message
	 * 6. commit the 2nd write operation
	 * 7. read the content of "file1", should be = initial messages then second message 
	 * 
	 * @throws IOException
	 * @throws NotBoundException
	 * @throws MessageNotFoundException
	 */
	public  static void customTest() throws IOException, NotBoundException, MessageNotFoundException{
		Client c = new Client();
		String fileName = "file1";

		char[] ss = "[INITIAL DATA!]".toCharArray(); // len = 15
		byte[] data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		c.write(fileName, data);

		c = new Client();
		ss = "File 1 test test END".toCharArray(); // len = 20
		data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		
		byte[] chunk = new byte[Configurations.CHUNK_SIZE];

		int seqN =data.length/Configurations.CHUNK_SIZE;
		int lastChunkLen = Configurations.CHUNK_SIZE;

		if (data.length%Configurations.CHUNK_SIZE > 0) {
			lastChunkLen = data.length%Configurations.CHUNK_SIZE;
			seqN++;
		}
		
		WriteAck ackMsg = c.masterStub.write(fileName);
		ReplicaServerClientInterface stub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());

		FileContent fileContent;
		@SuppressWarnings("unused")
		ChunkAck chunkAck;
		//		for (int i = 0; i < seqN; i++) {
		System.arraycopy(data, 0*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 0, fileContent);


		System.arraycopy(data, 1*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 1, fileContent);

		// read here 
		List<ReplicaLoc> locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read1 started ");

		// TODO fetch from all and verify 
		ReplicaLoc replicaLoc = locations.get(0);
		ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));


		// continue write 
		for(int i = 2; i < seqN-1; i++){
			System.arraycopy(data, i*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
			fileContent = new FileContent(fileName, chunk);
			chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
		}
		// copy the last chuck that might be < CHUNK_SIZE
		System.arraycopy(data, (seqN-1)*Configurations.CHUNK_SIZE, chunk, 0, lastChunkLen);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), seqN-1, fileContent);

		
		
		//commit
		ReplicaLoc primaryLoc = c.masterStub.locatePrimaryReplica(fileName);
		ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
		primaryStub.commit(ackMsg.getTransactionId(), seqN);

		
		// read
		locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read3 started ");

		replicaLoc = locations.get(0);
		replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));

	}

	public static Master startMaster() throws AccessException, RemoteException{
		Master master = new Master();
		MasterServerClientInterface stub = 
				(MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
		registry.rebind("MasterServerClientInterface", stub);
		System.err.println("Server ready");
		return master;
	}

	public static void main(String[] args) throws IOException {


		try {
			LocateRegistry.createRegistry(regPort);
			registry = LocateRegistry.getRegistry(regPort);

			Master master = startMaster();
			respawnReplicaServers(master);

			launchClients();

			for (String arg : args) {
				if (arg.equalsIgnoreCase("--auto-exit") || arg.equalsIgnoreCase("--exit")) {
					System.exit(0);
				}
				if (arg.equalsIgnoreCase("--gui")) {
					DFSGui.main(new String[0]);
					return;
				}
			}

			BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("\n=======================================================");
			System.out.println("  DFS Operations Completed Successfully!");
			System.out.println("=======================================================");
			System.out.println("Available interactive commands:");
			System.out.println("  gui                       - Launch Desktop GUI Interface");
			System.out.println("  read <filename>           - Read file content from DFS");
			System.out.println("  write <filename> <text>   - Write text to file in DFS");
			System.out.println("  upload <filepath>         - Upload local file to DFS with traffic calculation");
			System.out.println("  custom                    - Run custom test");
			System.out.println("  exit                      - Stop DFS servers and exit");
			System.out.println("=======================================================");

			while (true) {
				System.out.print("dfs> ");
				String line = consoleReader.readLine();
				if (line == null || line.trim().equalsIgnoreCase("exit") || line.trim().equalsIgnoreCase("quit")) {
					System.out.println("Exiting DFS...");
					System.exit(0);
				}
				line = line.trim();
				if (line.isEmpty()) continue;
				try {
					if (line.equalsIgnoreCase("gui")) {
						System.out.println("Opening Desktop GUI...");
						DFSGui.main(new String[0]);
					} else if (line.equalsIgnoreCase("custom")) {
						customTest();
					} else if (line.startsWith("read ")) {
						String fn = line.substring(5).trim();
						Client c = new Client();
						byte[] b = c.read(fn);
						System.out.println("[Content]: " + new String(b));
					} else if (line.startsWith("write ")) {
						String rest = line.substring(6).trim();
						int sp = rest.indexOf(' ');
						if (sp > 0) {
							String fn = rest.substring(0, sp).trim();
							String content = rest.substring(sp + 1);
							Client c = new Client();
							c.write(fn, content.getBytes());
							System.out.println("[Result]: Successfully written to " + fn);
						} else {
							System.out.println("Usage: write <filename> <text>");
						}
					} else if (line.startsWith("upload ")) {
						String localPath = line.substring(7).trim();
						java.io.File localFile = new java.io.File(localPath);
						if (!localFile.exists()) {
							System.out.println("[Error]: Local file not found: " + localPath);
						} else {
							byte[] fileBytes = java.nio.file.Files.readAllBytes(localFile.toPath());
							String remoteName = localFile.getName();
							TrafficMonitor.FileTrafficPlan plan = TrafficMonitor.calculateTrafficDistribution(remoteName, fileBytes.length, Configurations.CHUNK_SIZE);
							System.out.println("-------------------------------------------------------");
							System.out.println("[Traffic Calculation Plan for Upload]:");
							System.out.println("  Target File: " + remoteName + " (" + fileBytes.length + " bytes, " + plan.totalChunks + " chunks)");
							System.out.println("  Primary Node: Replica " + plan.primaryNodeId + " (receives initial streaming chunks)");
							System.out.println("  Slave Nodes:  " + plan.slaveNodeIds + " (receive 2PC commit replication)");
							System.out.println("  Projected Total Ops: " + plan.totalProjectedOps + " ops");
							for (int nid : plan.nodeExpectedOps.keySet()) {
								System.out.printf("    Node Replica %d: %d ops, %d bytes (%.1f%% workload)%n",
										nid, plan.nodeExpectedOps.get(nid), plan.nodeExpectedBytes.get(nid),
										plan.nodeWorkloadPercentage.getOrDefault(nid, 0.0));
							}
							System.out.println("-------------------------------------------------------");
							Client c = new Client();
							c.write(remoteName, fileBytes);
							System.out.println("[Result]: File '" + remoteName + "' uploaded and distributed across cluster replica nodes successfully!");
						}
					} else {
						System.out.println("Unknown command. Valid commands: read <file>, write <file> <text>, upload <filepath>, custom, gui, exit");
					}
				} catch (Exception e) {
					System.err.println("Command error: " + e.getMessage());
				}
			}

		} catch (RemoteException   e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}

}
