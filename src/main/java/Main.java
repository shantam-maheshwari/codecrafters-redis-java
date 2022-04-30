import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args) {
    int port = 6379;
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    ClientHandlerThread thread;

    try {
      serverSocket = new ServerSocket(port);
      while (true) {
        clientSocket = serverSocket.accept();
        thread = new ClientHandlerThread(clientSocket);
        thread.start();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}

class ClientHandlerThread extends Thread {
  private Socket clientSocket;

  public ClientHandlerThread(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }

  public void run() {
    BufferedReader in;
    PrintWriter out;
    String request;
    String response;

    try {
      in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      out = new PrintWriter(clientSocket.getOutputStream(), true);

      while ((request = in.readLine()) != null) {
        System.out.println("request: " + request);

        if (request.equals("ping")) {
          response = "+PONG\r\n";
          out.print(response);
          out.flush();
          System.out.println("response: " + response);
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}