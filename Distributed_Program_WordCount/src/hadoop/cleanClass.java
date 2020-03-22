package hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class cleanClass {

	public static boolean testCommand(String commande, int time) throws IOException, InterruptedException {
		boolean res = true;
		Process process = new ProcessBuilder().command(commande.split(" ")).inheritIO().start();
		boolean running = true;
		boolean tooLong = false;
		while (running) {
			boolean isDead = process.waitFor(time, TimeUnit.SECONDS);
			if(!isDead){
				tooLong = true;
				res = false;
				process.destroy();
			}
			running = !isDead && !tooLong;
		}
		return res;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		ArrayList<String> connectedComputer = new ArrayList<String>();
		String filename = "machines.txt";
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader(Paths.get(filename).toFile()));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String commande = "ssh"+" cochener@" + line + " 'hostname'";
			boolean res = testCommand(commande, 3);
			if (res) {
				connectedComputer.add(line);
				testCommand("ssh" + " cochener@" + line + " rm" + " -rf" + " /tmp/cochener/", 120);
			}
		}
	}

}
