import java.io.IOException;

public class UserFriendsTestDataManager {
    public static void main(String[] args) throws IOException {
        UserFriendsTestData testData10 = new UserFriendsTestData("testdata10.txt",10);
        UserFriendsTestData testData100 = new UserFriendsTestData("testdata100.txt",100);
        UserFriendsTestData testData1000 = new UserFriendsTestData();
        testData1000.writeToFile("testdata1000.txt",1000);
    }
}
