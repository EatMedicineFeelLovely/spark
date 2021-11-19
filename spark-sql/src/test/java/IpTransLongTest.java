public class IpTransLongTest {
    public static Long ipToNumber(String ip) {
        Long ipLong = 0L;
        String[] ipNumbers = ip.split("\\.");
        for (String ipNumber : ipNumbers) {
            ipLong = ipLong << 8 | Integer.parseInt(ipNumber);
        }
        return ipLong;
    }

    public static void main(String[] args) {
        System.out.println(ipToNumber("1.0.1.0"));
    }
}
