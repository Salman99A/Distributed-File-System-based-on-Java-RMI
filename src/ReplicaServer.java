import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicaServer implements ReplicaServerClientInterface,
		ReplicaMasterInterface, ReplicaReplicaInterface, Remote {

	
	private int regPort = Configurations.REG_PORT;
	private String regAddr = Configurations.REG_ADDR;
	
	private int id;
	private String dir;
	private Registry registry;
	
	private Map<Long, String> activeTxn; // map between active transactions and file names
	private Map<Long, Map<Long, byte[]>> txnFileMap; // map between transaction ID and corresponding file chunks
	private Map<String,	 List<ReplicaReplicaInterface> > filesReplicaMap; //replicas where files that this replica is its master are replicated  
	private Map<Integer, ReplicaLoc> replicaServersLoc; // Map<ReplicaID, replicaLoc>
	private Map<Integer, ReplicaReplicaInterface> replicaServersStubs; // Map<ReplicaID, replicaStub>
	private ConcurrentMap<String, ReentrantReadWriteLock> locks; // locks objects of the open files
	private volatile boolean alive = true;
	
	public ReplicaServer(int id, String dir) {
		this.id = id;
		this.dir = dir+"/Replica_"+id+"/";
		txnFileMap = new TreeMap<Long, Map<Long, byte[]>>();
		activeTxn = new TreeMap<Long, String>();
		filesReplicaMap = new TreeMap<String, List<ReplicaReplicaInterface>>();
		replicaServersLoc = new TreeMap<Integer, ReplicaLoc>();
		replicaServersStubs = new TreeMap<Integer, ReplicaReplicaInterface>();
		locks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
		
		File file = new File(this.dir);
		if (!file.exists()){
			file.mkdir();
		}
		
		try  {
			registry = LocateRegistry.getRegistry(regAddr, regPort);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void createFile(String fileName) throws IOException {
		if (!this.alive) throw new RemoteException("Replica " + id + " is OFFLINE (Node Failure).");
		File file = new File(dir+fileName);
		
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		ReentrantReadWriteLock lock = locks.get(fileName);
		
		lock.writeLock().lock();
		file.createNewFile();
		lock.writeLock().unlock();
	}

	@Override
	public FileContent read(String fileName) throws FileNotFoundException,
			RemoteException, IOException {
		if (!this.alive) throw new RemoteException("Replica " + id + " is OFFLINE (Node Failure).");
		File f = new File(dir+fileName);
		
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		ReentrantReadWriteLock lock = locks.get(fileName);
		
		@SuppressWarnings("resource")
		BufferedInputStream br = new BufferedInputStream(new FileInputStream(f));
		
		// assuming files are small and can fit in memory
		byte data[] = new byte[(int) (f.length())];
		
		lock.readLock().lock();
		br.read(data);
		lock.readLock().unlock();
		
		FileContent content = new FileContent(fileName, data);
		TrafficMonitor.recordRead(this.id, data.length);
		return content;
	}

	@Override
	public ChunkAck write(long txnID, long msgSeqNum, FileContent data)
			throws RemoteException, IOException {
		if (!this.alive) throw new RemoteException("Replica " + id + " is OFFLINE (Node Failure).");
		System.out.println("[@ReplicaServer] write "+msgSeqNum);
		// if this is not the first message of the write transaction
		if (!txnFileMap.containsKey(txnID)){
			txnFileMap.put(txnID, new TreeMap<Long, byte[]>());
			activeTxn.put(txnID, data.getFileName());
		}

		Map<Long, byte[]> chunkMap =  txnFileMap.get(txnID);
		chunkMap.put(msgSeqNum, data.getData());
		TrafficMonitor.recordWrite(this.id, data.getData().length);
		return new ChunkAck(txnID, msgSeqNum);
	}

	@Override
	public boolean commit(long txnID, long numOfMsgs)
			throws MessageNotFoundException, RemoteException, IOException {
		if (!this.alive) throw new RemoteException("Replica " + id + " is OFFLINE (Node Failure).");
		
		System.out.println("[@Replica] commit intiated");
		Map<Long, byte[]> chunkMap = txnFileMap.get(txnID);
		if (chunkMap.size() < numOfMsgs)
			throw new MessageNotFoundException();
		
		String fileName = activeTxn.get(txnID);
		List<ReplicaReplicaInterface> slaveReplicas = filesReplicaMap.get(fileName);
		
		if (slaveReplicas != null) {
			for (ReplicaReplicaInterface replica : slaveReplicas) {
				try {
					replica.reflectUpdate(txnID, fileName, new ArrayList<>(chunkMap.values()));
				} catch (Exception ex) {
					System.out.println("[@Replica] [FAILOVER] Slave replica unreachable during commit: " + ex.getMessage() + ". Continuing with surviving nodes.");
				}
			}
		}
		
		
		BufferedOutputStream bw =new BufferedOutputStream(new FileOutputStream(dir+fileName, true));
		
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		ReentrantReadWriteLock lock = locks.get(fileName);
		
		lock.writeLock().lock();
		for (Iterator<byte[]> iterator = chunkMap.values().iterator(); iterator.hasNext();) 
			bw.write(iterator.next());
		bw.close();
		lock.writeLock().unlock();
		
		
		if (slaveReplicas != null) {
			for (ReplicaReplicaInterface replica : slaveReplicas) {
				try {
					replica.releaseLock(fileName);
				} catch (Exception ignored) {}
			}
		}
		
		
		activeTxn.remove(txnID);
		txnFileMap.remove(txnID);
		
		return false;
	}

	@Override
	public boolean abort(long txnID) throws RemoteException {
		activeTxn.remove(txnID);
		filesReplicaMap.remove(txnID);
		return false;
	}


	@Override
	public boolean reflectUpdate(long txnID, String fileName, ArrayList<byte[]> data) throws IOException{
		if (!this.alive) throw new RemoteException("Replica " + id + " is OFFLINE (Node Failure).");
		System.out.println("[@Replica] reflect update initiated");
		BufferedOutputStream bw =new BufferedOutputStream(new FileOutputStream(dir+fileName, true));


		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		ReentrantReadWriteLock lock = locks.get(fileName);
		
		lock.writeLock().lock();
		try {
			for (Iterator<byte[]> iterator = data.iterator(); iterator.hasNext();) 
				bw.write(iterator.next());
		} finally {
			bw.close();
			lock.writeLock().unlock();
		}
		
		long totalReplicatedBytes = 0;
		for (byte[] b : data) totalReplicatedBytes += b.length;
		TrafficMonitor.recordWrite(this.id, totalReplicatedBytes);

		activeTxn.remove(txnID);
		return true;
	}

	@Override
	public void releaseLock(String fileName) {
		ReentrantReadWriteLock lock = locks.get(fileName);
		if (lock != null && lock.isWriteLockedByCurrentThread()) {
			lock.writeLock().unlock();
		}
	}

	@Override
	public void takeCharge(String fileName, List<ReplicaLoc> slaveReplicas) throws AccessException, RemoteException, NotBoundException {
		if (!this.alive) throw new RemoteException("Replica " + id + " is OFFLINE (Node Failure).");
		System.out.println("[@Replica] taking charge of file: "+fileName);
		System.out.println(slaveReplicas);
		
		List<ReplicaReplicaInterface> slaveReplicasStubs = new ArrayList<ReplicaReplicaInterface>(slaveReplicas.size());
		
		for (ReplicaLoc loc : slaveReplicas) {
			// if the current locations is this replica .. ignore
			if (loc.getId() == this.id)
				continue;
			  
			// if this is a new replica generate stub for this replica
			if (!replicaServersLoc.containsKey(loc.getId())){
				replicaServersLoc.put(loc.getId(), loc);
				ReplicaReplicaInterface stub = (ReplicaReplicaInterface) registry.lookup("ReplicaClient"+loc.getId());
				replicaServersStubs.put(loc.getId(), stub);
			}
			ReplicaReplicaInterface replicaStub = replicaServersStubs.get(loc.getId());
			slaveReplicasStubs.add(replicaStub);
		}
		
		filesReplicaMap.put(fileName, slaveReplicasStubs);
	}
	

	@Override
	public void setAlive(boolean isAlive) throws RemoteException {
		this.alive = isAlive;
		System.out.println("[@ReplicaServer " + this.id + "] State explicitly changed: alive=" + isAlive);
	}

	@Override
	public boolean isAlive() throws RemoteException {
		if (!this.alive) {
			throw new RemoteException("Replica " + id + " is OFFLINE (Node Failure).");
		}
		return true;
	}

	public int getId() {
		return id;
	}

	public String getDir() {
		return dir;
	}
	
}
