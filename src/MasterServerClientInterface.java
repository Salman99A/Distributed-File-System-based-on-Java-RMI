import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

public interface MasterServerClientInterface extends MasterInterface {

	/**
	 * Read file from server
	 * 
	 * @param fileName
	 * @return File data
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws RemoteException
	 */
	public List<ReplicaLoc> read(String fileName) throws FileNotFoundException,
			IOException, RemoteException;

	/**
	 * Start a new write transaction
	 * 
	 * @param fileName
	 * @return the required info
	 * @throws RemoteException
	 * @throws IOException
	 */
	public WriteAck write(String fileName) throws RemoteException, IOException;
	
	
	/**
	 * @param fileName
	 * @return the replica location of the primary replica of that file
	 * @throws RemoteException
	 */
	public ReplicaLoc locatePrimaryReplica(String fileName) throws RemoteException;

	/**
	 * Sets the alive status of a replica server and handles failover if needed
	 */
	public void setReplicaAlive(int replicaId, boolean isAlive) throws RemoteException;

	/**
	 * Returns list of all registered replica locations
	 */
	public List<ReplicaLoc> getReplicaLocations() throws RemoteException;

	/**
	 * Returns current primary replica map (fileName -> primaryReplicaLoc)
	 */
	public java.util.Map<String, ReplicaLoc> getPrimaryReplicaMap() throws RemoteException;

	/**
	 * Returns current files location map (fileName -> list of hosting replicas)
	 */
	public java.util.Map<String, List<ReplicaLoc>> getFilesLocationMap() throws RemoteException;

	/**
	 * Registers a replica server location and remote stub with the master
	 */
	public void registerReplicaServer(ReplicaLoc replicaLoc, ReplicaInterface replicaStub) throws RemoteException;
}
