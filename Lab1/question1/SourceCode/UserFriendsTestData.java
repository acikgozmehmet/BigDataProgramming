import com.github.javafaker.Faker;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class UserFriendsTestData {

    private List<String> users = new ArrayList<String>();
    private String filename = null;
    private int numberOfUsers = 2;
    private Map<String, List<String>> map = new TreeMap<>();
    private int[][] matrix;

    public UserFriendsTestData() {
    }

    public UserFriendsTestData(String filename, int numberOfUsers) throws IOException {
        this.filename = filename;
        this.numberOfUsers = numberOfUsers;
        writeToFile();
    }


    public void printMatrix() {
        System.out.println("users.toString() = " + users.toString());
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                System.out.print(matrix[i][j] + "\t");
            }
            System.out.println();
        }
    }


    private void createListOfUsers(int numOfUser) {
        Faker faker = new Faker();
        int count = 0;
        while (count < numOfUser) {
            String username = faker.name().firstName();
            if (!users.contains(username)) {
                users.add(username);
                count++;
            }
        }
        Collections.sort(users);
    }


    private void createFriendsForEachUser() {
        for (int userid = 0; userid < users.size(); userid++) {
            int numberOfFriends = new Random(System.nanoTime()).nextInt(users.size() / 2) + 2;
            int count = 0;
            while (count < numberOfFriends) {
                int id = new Random(System.nanoTime()).nextInt(users.size());
                if (id != userid) {
                    matrix[userid][id] = 1;
                    matrix[id][userid] = 1;
                    count++;
                }
            }
        }
    }


    private void matrixToMap() {
        for (int i = 0; i < matrix.length; i++) {
            List<String> friends = new ArrayList<>();
            for (int j = 0; j < matrix[i].length; j++) {
                if (matrix[i][j] == 1) {
                    friends.add(users.get(j));
                }
            }
            map.put(users.get(i), friends);
        }
    }


    public void writeToFile(String filename, int numberOfUsers) throws IOException {
        this.filename = filename;
        this.numberOfUsers = numberOfUsers;
        writeToFile();
    }

    private void writeToFile() throws IOException {
        FileWriter writer = new FileWriter(filename, false);

        matrix =  new int[numberOfUsers][numberOfUsers];
        createListOfUsers(numberOfUsers);
        createFriendsForEachUser();
        matrixToMap();


        for (String key : map.keySet()) {
            String value = map.get(key).toString().replace("[", "").replace("]", "").replace(",", "");
            writer.append(key + " -> " + value);
            writer.append("\n");
        }
        writer.flush();
        writer.close();
    }
} // end of class
