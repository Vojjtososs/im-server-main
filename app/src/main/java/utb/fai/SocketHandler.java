package utb.fai;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class SocketHandler {
    Socket mySocket;
    String clientID;
    String clientName = null;
    CopyOnWriteArraySet<String> groups = new CopyOnWriteArraySet<>();

    ActiveHandlers activeHandlers;
    ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<>(20);
    CountDownLatch startSignal = new CountDownLatch(2);

    OutputHandler outputHandler = new OutputHandler();
    InputHandler inputHandler = new InputHandler();
    volatile boolean inputFinished = false;

    public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
        this.mySocket = mySocket;
        clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
        this.activeHandlers = activeHandlers;

        groups.add("public");
    }

    class OutputHandler implements Runnable {
        public void run() {
            OutputStreamWriter writer;
            try {
                startSignal.countDown();
                startSignal.await();

                writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
                writer.write("Zadejte sve jmeno:\r\n");
                writer.flush();

                while (!inputFinished) {
                    String m = messages.take();
                    writer.write(m + "\r\n");
                    writer.flush();
                }
            } catch (IOException | InterruptedException e) {
            }
        }
    }

    class InputHandler implements Runnable {
        public void run() {
            try {
                startSignal.countDown();
                startSignal.await();

                activeHandlers.add(SocketHandler.this);
                BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
                String request = "";

                while ((request = reader.readLine()) != null) {
                    String trimReq = request.trim();
                    if (trimReq.isEmpty()) continue;

                    if (clientName == null) {
                        String[] parts = trimReq.split("\\s+");
                        String requestedName = "";

                        if (parts[0].toLowerCase().equals("#setmyname") && parts.length >= 2) {
                            requestedName = parts[1];
                        } else {
                            requestedName = parts[0];
                        }

                        if (!activeHandlers.isNameTaken(requestedName, SocketHandler.this)) {
                            clientName = requestedName;
                        }
                        continue;
                    }

                    String[] parts = trimReq.split("\\s+");
                    String cmd = parts[0].toLowerCase();

                    if (cmd.equals("#setmyname")) {
                        if (parts.length >= 2) {
                            String requestedName = parts[1];

                            if (!activeHandlers.isNameTaken(requestedName, SocketHandler.this)) {
                                clientName = requestedName;
                            }
                        }
                    } 
                    else if (cmd.equals("#sendprivate")) {
                        String[] privParts = trimReq.split("\\s+", 3);
                        if (privParts.length == 3) {
                            String targetName = privParts[1];
                            String msg = privParts[2];
                            String formattedMsg = "[" + clientName + "] >> " + msg;
                            activeHandlers.sendPrivateMessage(SocketHandler.this, targetName, formattedMsg);
                        }
                    } 
                    else if (cmd.equals("#join")) {
                        if (parts.length >= 2) {
                            groups.add(parts[1]);
                        }
                    } 
                    else if (cmd.equals("#leave")) {
                        if (parts.length >= 2) {
                            groups.remove(parts[1]);
                        }
                    } 
                    else if (cmd.equals("#groups")) {
                        String joinedGroups = String.join(",", groups);
                        messages.offer(joinedGroups);
                    } 
                    else {
                        String formattedMsg = "[" + clientName + "] >> " + request;
                        activeHandlers.sendMessageToGroups(SocketHandler.this, formattedMsg);
                    }
                }

                inputFinished = true;
                messages.offer("OutputHandler, wakeup and die!");
            } catch (IOException | InterruptedException e) {
            } finally {
                activeHandlers.remove(SocketHandler.this);
            }
        }
    }
}