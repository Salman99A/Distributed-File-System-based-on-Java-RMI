import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;


public class Master implements MasterReplicaInterface, MasterServerClientInterface, Remote{

	class HeartBeatTask extends TimerTask {

		@Override
		public void run() {
			// check state of replicas
			for (ReplicaLoc replicaLoc : replicaServersLocs) {
				try {
					ReplicaMasterInterface stub = getReplicaStub(replicaLoc.getId());
					if (stub != null) {
						stub.isAlive();
						replicaLoc.setAlive(true);
					}
				} catch (RemoteException e) {
					if (replicaLoc.isAlive()) {
						try {
							setReplicaAlive(replicaLoc.getId(), false);
						} catch (Exception ignored) {}
					} else {
						replicaLoc.setAlive(false);
					}
				}
			}
		}
	}
	
	private int nextTID;
	private int heartBeatRate = Configurations.HEART_BEAT_RATE;
	private int replicationN = Configurations.REPLICATION_N; // number of file replicas
	private Timer HeartBeatTimer;
	private Random randomGen;

	private Map<String,	 List<ReplicaLoc> > filesLocationMap;
	private Map<String,	 ReplicaLoc> primaryReplicaMap;
	//	private Map<Integer, String> activeTransactions; // active transactions <ID, fileName>
	private List<ReplicaLoc> replicaServersLocs;
	private List<ReplicaMasterInterface> replicaServersStubs; 


	public Master() {
		filesLocationMap = new HashMap<String, List<ReplicaLoc>>();
		primaryReplicaMap = new HashMap<String, ReplicaLoc>();
//		activeTransactions = new HashMap<Integer, String>();
		replicaServersLocs = new ArrayList<ReplicaLoc>();
		replicaServersStubs = new ArrayList<ReplicaMasterInterface>();

		nextTID = 0;
		randomGen = new Random();
		
		HeartBeatTimer = new Timer();  //At this line a new Thread will be created
		HeartBeatTimer.scheduleAtFixedRate(new HeartBeatTask(), 0, heartBeatRate); //delay in milliseconds
	}

	public ReplicaMasterInterface getReplicaStub(int id) {
		for (int i = 0; i < replicaServersLocs.size(); i++) {
			if (replicaServersLocs.get(i).getId() == id && i < replicaServersStubs.size()) {
				return replicaServersStubs.get(i);
			}
		}
		return null;
	}

	public ReplicaLoc getReplicaLoc(int id) {
		for (ReplicaLoc loc : replicaServersLocs) {
			if (loc.getId() == id) {
				return loc;
			}
		}
		return null;
	}

	/**
	 * elects a new primary replica for the given file
	 * @param fileName
	 */
	private void assignNewMaster(String fileName){
		List<ReplicaLoc> replicas = filesLocationMap.get(fileName);
		ReplicaLoc oldPrimary = primaryReplicaMap.get(fileName);
		int oldId = (oldPrimary != null) ? oldPrimary.getId() : -1;
		boolean newPrimaryAssigned = false;
		if (replicas != null) {
			for (ReplicaLoc replicaLoc : replicas) {
				if (replicaLoc.isAlive()){
					newPrimaryAssigned = true;
					primaryReplicaMap.put(fileName, replicaLoc);
					TrafficMonitor.recordFailover(oldId, replicaLoc.getId(), fileName, "Primary replica failure");
					System.out.println("[@Master] [FAILOVER] Traffic transferred: New Primary Replica for '" + fileName + "' is Replica " + replicaLoc.getId());
					try {
						ReplicaMasterInterface stub = getReplicaStub(replicaLoc.getId());
						if (stub != null) stub.takeCharge(fileName, filesLocationMap.get(fileName));
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}

			// If none of the existing replicas for this file are alive, recruit another alive node in cluster
			if (!newPrimaryAssigned) {
				for (ReplicaLoc loc : replicaServersLocs) {
					if (loc.isAlive()) {
						newPrimaryAssigned = true;
						if (!replicas.contains(loc)) {
							replicas.add(loc);
						}
						primaryReplicaMap.put(fileName, loc);
						TrafficMonitor.recordFailover(oldId, loc.getId(), fileName, "Cluster-wide failover to surviving node");
						System.out.println("[@Master] [FAILOVER] Cluster-wide Traffic transferred: New Primary Replica for '" + fileName + "' is Replica " + loc.getId());
						try {
							ReplicaMasterInterface stub = getReplicaStub(loc.getId());
							if (stub != null) stub.takeCharge(fileName, replicas);
						} catch (Exception e) {
							e.printStackTrace();
						}
						break;
					}
				}
			}
		}
	}

	/**
	 * creates a new file @ N replica servers chosen by lowest traffic workload
	 * elects the primary replica based on least loaded node
	 * @param fileName
	 */
	private void createNewFile(String fileName) throws IOException {
		System.out.println("[@Master] Creating new file initiated");

		// Find all currently alive replicas in strict range [0..4]
		List<Integer> aliveIndices = new ArrayList<Integer>();
		for (ReplicaLoc loc : replicaServersLocs) {
			int id = loc.getId();
			if (loc.isAlive() && id >= 0 && id < 5 && !aliveIndices.contains(id)) {
				aliveIndices.add(id);
			}
		}

		if (aliveIndices.isEmpty()) {
			throw new IOException("No alive replica servers available in cluster (Nodes 0-4)!");
		}

		// Sort alive replicas by lowest traffic load (total ops, then bytes) to balance cluster traffic
		aliveIndices.sort((a, b) -> {
			long opsA = TrafficMonitor.getTotalOps(a);
			long opsB = TrafficMonitor.getTotalOps(b);
			if (opsA != opsB) return Long.compare(opsA, opsB);
			return Long.compare(TrafficMonitor.getBytesTransferred(a), TrafficMonitor.getBytesTransferred(b));
		});

		int numToPick = Math.min(replicationN, aliveIndices.size());
		List<ReplicaLoc> replicas = new ArrayList<ReplicaLoc>();
		int luckyReplicas[] = new int[numToPick];

		for (int i = 0; i < numToPick; i++) {
			int luckyId = aliveIndices.get(i);
			luckyReplicas[i] = luckyId;
			ReplicaLoc locObj = getReplicaLoc(luckyId);
			if (locObj == null) locObj = new ReplicaLoc(luckyId, "127.0.0.1", true);
			replicas.add(locObj);

			// create the file at the lucky replicas
			try {
				ReplicaMasterInterface stub = getReplicaStub(luckyId);
				if (stub != null) stub.createFile(fileName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// the primary replica is the least-loaded alive replica (first in sorted list)
		int primary = luckyReplicas[0];
		try {
			ReplicaMasterInterface primaryStub = getReplicaStub(primary);
			if (primaryStub != null) primaryStub.takeCharge(fileName, replicas);
		} catch (Exception e) {
			e.printStackTrace();
		}

		filesLocationMap.put(fileName, replicas);
		ReplicaLoc primaryLoc = getReplicaLoc(primary);
		if (primaryLoc == null) primaryLoc = new ReplicaLoc(primary, "127.0.0.1", true);
		primaryReplicaMap.put(fileName, primaryLoc);
	}

	
	@Override
	public List<ReplicaLoc> read(String fileName) throws FileNotFoundException,
	IOException, RemoteException {
		List<ReplicaLoc> replicaLocs = filesLocationMap.get(fileName);
		if (replicaLocs == null)
			throw new FileNotFoundException("File not found in DFS metadata: " + fileName);

		// Check if any replica hosting this file is currently alive
		boolean anyAlive = false;
		for (ReplicaLoc rl : replicaLocs) {
			if (rl.isAlive()) {
				anyAlive = true;
				break;
			}
		}

		// If all replicas hosting this file are down, but the cluster has alive nodes:
		if (!anyAlive) {
			ReplicaLoc aliveCandidate = null;
			for (ReplicaLoc rl : replicaServersLocs) {
				if (rl.isAlive()) {
					aliveCandidate = rl;
					break;
				}
			}

			if (aliveCandidate != null) {
				File tgtDir = new File("./Replica_" + aliveCandidate.getId());
				if (!tgtDir.exists()) tgtDir.mkdirs();
				File tgtFile = new File(tgtDir, fileName);

				if (!tgtFile.exists() || tgtFile.length() == 0) {
					for (int srcId = 0; srcId < 5; srcId++) {
						File srcFile = new File("./Replica_" + srcId, fileName);
						if (srcFile.exists() && srcFile.length() > 0) {
							try {
								java.nio.file.Files.copy(srcFile.toPath(), tgtFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
								break;
							} catch (Exception ignored) {}
						}
					}
				}

				if (!replicaLocs.contains(aliveCandidate)) {
					replicaLocs.add(aliveCandidate);
				}
				primaryReplicaMap.put(fileName, aliveCandidate);
				TrafficMonitor.recordFailover(-1, aliveCandidate.getId(), fileName, "Read failover: file recruited to alive node " + aliveCandidate.getId());
			}
		}

		// Prioritize alive replicas so traffic transfers away from failed nodes
		List<ReplicaLoc> prioritized = new ArrayList<>(replicaLocs);
		prioritized.sort((a, b) -> Boolean.compare(!a.isAlive(), !b.isAlive()));
		return prioritized;
	}

	@Override
	public WriteAck write(String fileName) throws RemoteException, IOException {
		System.out.println("[@Master] write request initiated");
		long timeStamp = System.currentTimeMillis();

		List<ReplicaLoc> replicaLocs = filesLocationMap.get(fileName);
		int tid = nextTID++;
		if (replicaLocs == null) {
			createNewFile(fileName);
		}

		ReplicaLoc primaryReplicaLoc = primaryReplicaMap.get(fileName);

		// If the primary replica is down or null .. elect a new replica from surviving alive nodes
		if (primaryReplicaLoc == null || !primaryReplicaLoc.isAlive()) {
			assignNewMaster(fileName);
			primaryReplicaLoc = primaryReplicaMap.get(fileName);
		}

		if (primaryReplicaLoc == null || !primaryReplicaLoc.isAlive()) {
			throw new IOException("Cannot perform write: All replica servers in cluster are OFFLINE!");
		}

		return new WriteAck(tid, timeStamp, primaryReplicaLoc);
	}

	@Override
	public ReplicaLoc locatePrimaryReplica(String fileName)
			throws RemoteException {
		
		return primaryReplicaMap.get(fileName);
	}
	

	/**
	 * registers new replica server @ the master by adding required meta data
	 * @param replicaLoc
	 * @param replicaStub
	 */
	@Override
	public synchronized void registerReplicaServer(ReplicaLoc replicaLoc, ReplicaInterface replicaStub) throws RemoteException {
		int id = replicaLoc.getId();
		if (id < 0 || id >= 5) {
			System.err.println("[@Master] Strictly ignoring replica ID " + id + ". Only Replicas 0-4 are permitted.");
			return;
		}

		// De-duplicate: replace existing if already registered
		for (int i = 0; i < replicaServersLocs.size(); i++) {
			if (replicaServersLocs.get(i).getId() == id) {
				replicaServersLocs.set(i, replicaLoc);
				replicaServersStubs.set(i, (ReplicaMasterInterface) replicaStub);
				return;
			}
		}

		replicaServersLocs.add(replicaLoc);
		replicaServersStubs.add((ReplicaMasterInterface) replicaStub);
	}

	@Override
	public Map<String, List<ReplicaLoc>> getFilesLocationMap() throws RemoteException {
		return filesLocationMap;
	}

	@Override
	public Map<String, ReplicaLoc> getPrimaryReplicaMap() throws RemoteException {
		return primaryReplicaMap;
	}

	public List<ReplicaLoc> getReplicaServersLocs() {
		return replicaServersLocs;
	}

	public List<ReplicaMasterInterface> getReplicaServersStubs() {
		return replicaServersStubs;
	}

	@Override
	public void setReplicaAlive(int replicaId, boolean isAlive) throws RemoteException {
		System.out.println("[@Master] Setting Replica " + replicaId + " alive=" + isAlive);
		for (ReplicaLoc loc : replicaServersLocs) {
			if (loc.getId() == replicaId) {
				loc.setAlive(isAlive);
				break;
			}
		}
		for (List<ReplicaLoc> locs : filesLocationMap.values()) {
			for (ReplicaLoc loc : locs) {
				if (loc.getId() == replicaId) {
					loc.setAlive(isAlive);
				}
			}
		}
		try {
			ReplicaMasterInterface stub = getReplicaStub(replicaId);
			if (stub != null) {
				stub.setAlive(isAlive);
			}
		} catch (Exception ex) {
			System.err.println("[@Master] Could not reach stub for Replica " + replicaId + ": " + ex.getMessage());
		}

		// Update global traffic monitor with ground-truth topology
		Map<Integer, Boolean> aliveMap = new HashMap<>();
		for (ReplicaLoc loc : replicaServersLocs) {
			aliveMap.put(loc.getId(), loc.isAlive());
		}
		TrafficMonitor.updateClusterTopology(aliveMap);

		List<Integer> aliveList = new ArrayList<>();
		for (ReplicaLoc loc : replicaServersLocs) {
			if (loc.isAlive()) aliveList.add(loc.getId());
		}

		if (!isAlive) {
			if (aliveList.isEmpty()) {
				System.err.println("[@Master] [CRITICAL] All replica servers are OFFLINE!");
				TrafficMonitor.recordFailover(replicaId, -1, "Cluster", "All replica nodes offline!");
				return;
			}

			if (aliveList.size() == 1) {
				int soleSurvivor = aliveList.get(0);
				System.out.println("[@Master] [CRITICAL FAILOVER] Only Replica " + soleSurvivor + " is ONLINE! Transferring 100% of cluster traffic & file replicas to Replica " + soleSurvivor);
				TrafficMonitor.recordFailover(replicaId, soleSurvivor, "Cluster Workload", 
						"Sole surviving node: 100% of cluster traffic transferred to Replica " + soleSurvivor);

				// Ensure all files and primary roles are transferred to the sole survivor
				healAllFilesToSoleSurvivor(soleSurvivor);

			} else {
				// Multiple nodes still alive
				int targetId = TrafficMonitor.getFailoverTarget(replicaId);
				if (targetId >= 0) {
					System.out.println("[@Master] [FAILOVER] Traffic workload of Node " + replicaId + " transferred to Replica " + targetId);
				}

				// If primary replica went down, reassign primary to transfer write traffic
				for (Map.Entry<String, ReplicaLoc> entry : new HashMap<>(primaryReplicaMap).entrySet()) {
					if (entry.getValue().getId() == replicaId) {
						System.out.println("[@Master] [FAILOVER] Primary Replica " + replicaId + " for file '" + entry.getKey() + "' went down. Assigning new primary...");
						assignNewMaster(entry.getKey());
					}
				}

				// Self-Healing file transfer: Re-replicate files to ensure 100% availability
				healFilesAfterNodeFailure(replicaId, targetId);
			}

		} else {
			// When revived, restore normal traffic workload
			System.out.println("[@Master] Node " + replicaId + " revived: cluster topology updated (" + aliveList.size() + " alive).");
			TrafficMonitor.recordFailover(replicaId, replicaId, "Cluster", "Node " + replicaId + " revived and rejoined cluster.");
		}
	}

	private void healAllFilesToSoleSurvivor(int soleSurvivor) {
		ReplicaLoc soleLoc = null;
		for (ReplicaLoc loc : replicaServersLocs) {
			if (loc.getId() == soleSurvivor) {
				soleLoc = loc;
				break;
			}
		}
		if (soleLoc == null) return;

		for (Map.Entry<String, List<ReplicaLoc>> entry : new HashMap<>(filesLocationMap).entrySet()) {
			String fileName = entry.getKey();
			List<ReplicaLoc> fileReplicas = entry.getValue();

			// 1. Assign primary to soleSurvivor
			primaryReplicaMap.put(fileName, soleLoc);

			// 2. Add soleSurvivor to fileReplicas if not present
			boolean hasSole = false;
			for (ReplicaLoc rl : fileReplicas) {
				if (rl.getId() == soleSurvivor) {
					hasSole = true;
					break;
				}
			}
			if (!hasSole) {
				fileReplicas.add(soleLoc);
			}

			// 3. Ensure file exists in ./Replica_<soleSurvivor>/fileName
			File tgtDir = new File("./Replica_" + soleSurvivor);
			if (!tgtDir.exists()) tgtDir.mkdirs();
			File tgtFile = new File(tgtDir, fileName);

			if (!tgtFile.exists() || tgtFile.length() == 0) {
				// Copy from any available replica folder (0 to 4)
				for (int srcId = 0; srcId < 5; srcId++) {
					if (srcId == soleSurvivor) continue;
					File srcFile = new File("./Replica_" + srcId, fileName);
					if (srcFile.exists() && srcFile.length() > 0) {
						try {
							java.nio.file.Files.copy(srcFile.toPath(), tgtFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
							System.out.println("[@Master] [HEALING] Successfully copied file '" + fileName + "' to sole surviving Replica " + soleSurvivor + " from Replica " + srcId);
							break;
						} catch (Exception ex) {
							System.err.println("[@Master] Error copying file to sole survivor: " + ex.getMessage());
						}
					}
				}
			}

			// 4. Tell stub to take charge
			try {
				ReplicaMasterInterface stub = getReplicaStub(soleSurvivor);
				if (stub != null) {
					stub.takeCharge(fileName, Collections.singletonList(soleLoc));
				}
			} catch (Exception ex) {
				System.err.println("[@Master] Error in takeCharge for sole survivor: " + ex.getMessage());
			}
		}
	}

	private void healFilesAfterNodeFailure(int failedNodeId, int defaultTargetId) {
		for (Map.Entry<String, List<ReplicaLoc>> entry : new HashMap<>(filesLocationMap).entrySet()) {
			String fileName = entry.getKey();
			List<ReplicaLoc> fileReplicas = entry.getValue();
			boolean hasFailedNode = false;
			for (ReplicaLoc rl : fileReplicas) {
				if (rl.getId() == failedNodeId) {
					hasFailedNode = true;
					break;
				}
			}

			if (hasFailedNode) {
				// Find an alive node already hosting this file to read from
				ReplicaLoc aliveSource = null;
				for (ReplicaLoc rl : fileReplicas) {
					if (rl.isAlive() && rl.getId() != failedNodeId) {
						aliveSource = rl;
						break;
					}
				}

				// Find a healthy node in the cluster that does not yet host this file
				ReplicaLoc targetNode = null;
				for (ReplicaLoc candidate : replicaServersLocs) {
					if (candidate.isAlive() && candidate.getId() != failedNodeId) {
						boolean alreadyHas = false;
						for (ReplicaLoc rl : fileReplicas) {
							if (rl.getId() == candidate.getId()) {
								alreadyHas = true;
								break;
							}
						}
						if (!alreadyHas) {
							targetNode = candidate;
							break;
						}
					}
				}

				if (targetNode != null && aliveSource != null) {
					try {
						System.out.println("[@Master] [HEALING] Transferring file '" + fileName + "' replica from Node " + failedNodeId + " to Node " + targetNode.getId() + "...");
						File srcDir = new File("./Replica_" + aliveSource.getId());
						File srcFile = new File(srcDir, fileName);
						if (srcFile.exists()) {
							byte[] fileBytes = java.nio.file.Files.readAllBytes(srcFile.toPath());
							File tgtDir = new File("./Replica_" + targetNode.getId());
							if (!tgtDir.exists()) tgtDir.mkdirs();
							File tgtFile = new File(tgtDir, fileName);
							try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tgtFile)) {
								fos.write(fileBytes);
							}

							// Update file locations: replace failed replica with targetNode
							for (int i = 0; i < fileReplicas.size(); i++) {
								if (fileReplicas.get(i).getId() == failedNodeId) {
									fileReplicas.set(i, targetNode);
									break;
								}
							}

							TrafficMonitor.recordFailover(failedNodeId, targetNode.getId(), fileName, "File replica transferred to maintain 100% HA");
							System.out.println("[@Master] [HEALING] File '" + fileName + "' successfully transferred to Node " + targetNode.getId() + "!");
						}
					} catch (Exception ex) {
						System.err.println("[@Master] [HEALING ERROR] " + ex.getMessage());
					}
				}
			}
		}
	}

	@Override
	public List<ReplicaLoc> getReplicaLocations() throws RemoteException {
		return new ArrayList<>(replicaServersLocs);
	}

}
