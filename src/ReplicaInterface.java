import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ReplicaInterface extends Remote {

    public void setAlive(boolean isAlive) throws RemoteException;

    public boolean isAlive() throws RemoteException;

}
