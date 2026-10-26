package utb.fai;

import java.util.HashSet;

public class ActiveHandlers {
    private static final long serialVersionUID = 1L;
    private HashSet<SocketHandler> activeHandlersSet = new HashSet<SocketHandler>();

    synchronized void sendMessageToGroups(SocketHandler sender, String message) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != sender && handler.clientName != null) {
                
                boolean sharesGroup = false;
                for (String group : sender.groups) {
                    if (handler.groups.contains(group)) {
                        sharesGroup = true;
                        break;
                    }
                }

                if (sharesGroup) {
                    if (!handler.messages.offer(message)) {
                        System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
                    }
                }
            }
        }
    }

    synchronized void sendPrivateMessage(SocketHandler sender, String targetName, String message) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler.clientName != null && handler.clientName.equals(targetName)) {
                if (!handler.messages.offer(message)) {
                    System.err.printf("Client %s message queue is full, dropping the message!\n", handler.clientID);
                }
            }
        }
    }

    synchronized boolean add(SocketHandler handler) {
        return activeHandlersSet.add(handler);
    }

    synchronized boolean remove(SocketHandler handler) {
        return activeHandlersSet.remove(handler);
    }

    synchronized boolean isNameTaken(String name, SocketHandler excludeHandler) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != excludeHandler && handler.clientName != null) {
                if (handler.clientName.equalsIgnoreCase(name)) {
                    return true; 
                }
            }
        }
        return false; 
    }
}