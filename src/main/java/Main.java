import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class Main {
  public static void main(String[] args) {
    Map<String, String> dataStore = new HashMap<>();

    int port = 6379;
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    ClientHandlerThread thread;

    try {
      serverSocket = new ServerSocket(port);
      while (true) {
        clientSocket = serverSocket.accept();
        thread = new ClientHandlerThread(clientSocket, dataStore);
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
  Map<String, String> dataStore;

  public ClientHandlerThread(Socket clientSocket, Map<String, String> dataStore) {
    this.clientSocket = clientSocket;
    this.dataStore = dataStore;
  }

  public void run() {
    BufferedReader in;
    PrintWriter out;

    int requestBulkStringArrayLength;
    String requestBulkString;
    int requestBulkStringLength;

    // PING, SET
    String responseSimpleString;

    // ECHO, GET
    String responseBulkString;
    int responseBulkStringLength;

    // SET, GET
    String key;
    String value;

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
            // get length of bulk string
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }
            requestBulkStringLength = Integer.parseInt(requestBulkString.substring(1));

            // get bulk string (message)
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }

            // send bulk string
            responseBulkStringLength = requestBulkStringLength;
            responseBulkString = "$" + responseBulkStringLength + "\r\n" + requestBulkString + "\r\n";
            out.print(responseBulkString);
            out.flush();
          }

          // SET
          else if (requestBulkString.equals("set")) {
            // get length of bulk string
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }
            requestBulkStringLength = Integer.parseInt(requestBulkString.substring(1));

            // get bulk string (key)
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }
            key = requestBulkString;

            // get length of bulk string
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }
            requestBulkStringLength = Integer.parseInt(requestBulkString.substring(1));

            // get bulk string (value)
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }
            value = requestBulkString;

            // set key, value
            dataStore.put(key, value);

            // send simple string
            responseSimpleString = "+OK\r\n";
            out.print(responseSimpleString);
            out.flush();
          }

          // GET
          else if (requestBulkString.equals("get")) {
            // get length of bulk string
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }
            System.out.println("key length: " + requestBulkString);
            requestBulkStringLength = Integer.parseInt(requestBulkString.substring(1));

            // get bulk string (key)
            requestBulkString = in.readLine();
            if (requestBulkString == null) {
              break;
            }
            System.out.println("key: " + requestBulkString);
            key = requestBulkString;

            // get value
            value = dataStore.getOrDefault(key, "(nil)");
            System.out.println("value: " + value);

            // send bulk string
            responseBulkStringLength = value.length();
            responseBulkString = "$" + responseBulkStringLength + "\r\n" + value + "\r\n";
            System.out.println(responseBulkString);
            out.print(responseBulkString);
            out.flush();
          }
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}