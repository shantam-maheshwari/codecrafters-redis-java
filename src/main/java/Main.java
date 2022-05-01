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
  private Map<String, String> dataStore;
  private Integer requestBulkStringArrayLength;

  public ClientHandlerThread(Socket clientSocket, Map<String, String> dataStore) {
    this.clientSocket = clientSocket;
    this.dataStore = dataStore;
  }

  private String readRequestBulkStringData(BufferedReader in) {
    int ignore = readRequestBulkStringLength(in);
    return _readRequestBulkStringData(in);
  }

  private String _readRequestBulkStringData(BufferedReader in) {
    String requestBulkString = null;
    try {
      requestBulkString = in.readLine();
      if (requestBulkString == null) {
        throw new Exception("Expected bulk string!");
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("IllegalRequestException: " + e.getMessage());
    }
    requestBulkStringArrayLength--;
    return requestBulkString;
  }

  private Integer readRequestBulkStringLength(BufferedReader in) {
    String requestBulkString = null;
    try {
      requestBulkString = in.readLine();
      if (requestBulkString == null) {
        throw new Exception("Expected $n, where n is length of bulk string!");
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("IllegalRequestException: " + e.getMessage());
    }
    return Integer.parseInt(requestBulkString.substring(1));
  }

  private void writeResponseBulkString(String responseBulkStringData, PrintWriter out) {
    // handle null bulk strings
    int responseBulkStringLength = responseBulkStringData.length();
    if (responseBulkStringLength == 0) {
      responseBulkStringLength = -1;
    }

    String responseBulkString = "$" + responseBulkStringLength + "\r\n" + responseBulkStringData + "\r\n";
    out.print(responseBulkString);
    out.flush();
  }

  private void writeResponseSimpleString(String responseSimpleStringData, PrintWriter out) {
    String responseSimpleString = "+" + responseSimpleStringData + "\r\n";
    out.print(responseSimpleString);
    out.flush();
  }

  public void run() {
    BufferedReader in;
    PrintWriter out;

    String requestBulkStringData;

    // PING, SET
    String responseSimpleStringData;

    // ECHO, GET
    String responseBulkStringData;

    // SET, GET
    String key;
    String value;

    // SET PX
    long millis;
    ExpireKeyThread thread;

    try {
      in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      out = new PrintWriter(clientSocket.getOutputStream(), true);

      while ((requestBulkStringData = in.readLine()) != null) {

        // get length of array of bulk strings
        requestBulkStringArrayLength = Integer.parseInt(requestBulkStringData.substring(1));

        // get command
        requestBulkStringData = readRequestBulkStringData(in);

        // PING
        if (requestBulkStringData.equals("ping")) {
          // send simple string
          responseSimpleStringData = "PONG";
          writeResponseSimpleString(responseSimpleStringData, out);
        }

        // ECHO
        else if (requestBulkStringData.equals("echo")) {

          // get message
          requestBulkStringData = readRequestBulkStringData(in);

          // send bulk string
          responseBulkStringData = requestBulkStringData;
          writeResponseBulkString(responseBulkStringData, out);
        }

        // SET
        else if (requestBulkStringData.equals("set")) {

          // get key
          requestBulkStringData = readRequestBulkStringData(in);
          key = requestBulkStringData;

          // get value
          requestBulkStringData = readRequestBulkStringData(in);
          value = requestBulkStringData;

          // set key, value
          dataStore.put(key, value);

          // send simple string
          responseSimpleStringData = "OK";
          writeResponseSimpleString(responseSimpleStringData, out);

          // SET PX
          if (requestBulkStringArrayLength > 0) {

            // get option
            requestBulkStringData = readRequestBulkStringData(in);

            if (requestBulkStringData.equals("px")) {
              // get milliseconds
              requestBulkStringData = readRequestBulkStringData(in);
              millis = Long.parseLong(requestBulkStringData);

              thread = new ExpireKeyThread(key, millis, dataStore);
              thread.start();
            }
          }
        }

        // GET
        else if (requestBulkStringData.equals("get")) {

          // get key
          requestBulkStringData = readRequestBulkStringData(in);
          key = requestBulkStringData;

          // get value
          responseBulkStringData = dataStore.getOrDefault(key, "");

          // send bulk string
          writeResponseBulkString(responseBulkStringData, out);
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}

class ExpireKeyThread extends Thread {
  private Map<String, String> dataStore;
  private String key;
  private long millis;

  public ExpireKeyThread(String key, long millis, Map<String, String> dataStore) {
    this.key = key;
    this.millis = millis;
    this.dataStore = dataStore;
  }

  public void run() {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      System.out.println("InterruptedException: " + e.getMessage());
    }
    dataStore.remove(key);
  }
}