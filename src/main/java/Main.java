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

    int requestBulkStringArrayLength;
    String requestBulkString;
    int requestBulkStringLength;

    String responseBulkStringArray;
    int responseBulkStringArrayLength;
    String responseSimpleString;
    String responseBulkString;
    int responseBulkStringLength;

    try {
      in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      out = new PrintWriter(clientSocket.getOutputStream(), true);

      while ((requestBulkString = in.readLine()) != null) {
        // get length of array of bulk strings
        requestBulkStringArrayLength = Integer.parseInt(requestBulkString.substring(1));

        for (int i = 0; i < requestBulkStringArrayLength; i++) {
          // get length of bulk string
          requestBulkString = in.readLine();
          if (requestBulkString == null) {
            break;
          }
          requestBulkStringLength = Integer.parseInt(requestBulkString.substring(1));

          // get bulk string (command)
          requestBulkString = in.readLine();
          if (requestBulkString == null) {
            break;
          }

          // PING
          if (requestBulkString.equals("ping")) {
            // send simple string
            responseSimpleString = "+PONG\r\n";
            out.print(responseSimpleString);
            out.flush();
          }

          // ECHO
          else if (requestBulkString.equals("echo")) {
            // get bulk string (message)
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }

            // send array of bulk strings
            responseBulkStringArrayLength = 1;
            responseBulkString = requestBulkString;
            responseBulkStringLength = requestBulkStringLength;
            responseBulkStringArray = "*" + responseBulkStringArrayLength + "\r\n$" + responseBulkStringLength + "\r\n"
                + responseBulkString + "\r\n";
            out.print(responseBulkStringArray);
            out.flush();
          }
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}