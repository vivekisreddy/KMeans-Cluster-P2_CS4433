package kmeans;

public class Centroid {
    public double x;
    public double y;

    public Centroid(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double distance(double px, double py) {
        return Math.sqrt(Math.pow(px - x, 2) + Math.pow(py - y, 2));
    }

    @Override
    public String toString() {
        return x + "," + y;
    }

    public static double distance(Centroid c1, Centroid c2) {
        return Math.sqrt(Math.pow(c1.x - c2.x, 2) + Math.pow(c1.y - c2.y, 2));
    }
}
