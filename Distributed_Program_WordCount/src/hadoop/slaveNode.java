package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class slaveNode {

	public static boolean testCommand(String commande, int time) throws IOException, InterruptedException {
		boolean res = true;
		Process process = new ProcessBuilder().command(commande.split(" ")).inheritIO().start();
		boolean running = true;
		boolean tooLong = false;
		while (running) {
			boolean isDead = process.waitFor(time, TimeUnit.SECONDS);
			if (!isDead) {
				tooLong = true;
				res = false;
				process.destroy();
			}
			running = !isDead && !tooLong;
		}
		return res;
	}

	// map
	public static ArrayList<Map.Entry<String, Integer>> Map(String filename) throws IOException {
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader(Paths.get(filename).toFile()));
		ArrayList<Map.Entry<String, Integer>> counts = new ArrayList<Map.Entry<String, Integer>>();
		String line = null;
		while ((line = reader.readLine()) != null) {
			for (String word : line.split(" ")) {
				if (word.isEmpty()) {
					continue;
				}
				counts.add(new AbstractMap.SimpleEntry<String, Integer>(word, 1));
			}
		}
		return counts;
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		if (args[0].equals("0") && args.length == 3) {
			String fileIndice = args[1];
			String filename = args[2];
			ArrayList<Entry<String, Integer>> res = Map(filename);
			String content = "";
			int i = 0;
			while (i < res.size()) {
				content = content.concat(res.get(i).getKey() + " " + res.get(i).getValue());
				i += 1;
				if (i < res.size()) {
					content = content.concat("\n");
				}
			}
			String outputFile = "/tmp/cochener/maps/UM" + fileIndice + ".txt";
			File newTextFile = new File(outputFile);
			FileWriter fw = new FileWriter(newTextFile);
			fw.write(content);
			fw.close();
		}
		if (args[0].contentEquals("1") && args.length == 3) {
			String filename = args[2];

			// Obtention du nbMachines
			@SuppressWarnings("resource")
			BufferedReader readerMachines = new BufferedReader(
					new FileReader(Paths.get("/tmp/cochener/machines.txt").toFile()));
			ArrayList<String> machines = new ArrayList<String>();
			String line2 = null;
			while ((line2 = readerMachines.readLine()) != null) {
				machines.add(line2);
			}
			int nbMachines = machines.size();

			// Création des fichiers avec les mêmes hashcode
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader(new FileReader(Paths.get(filename).toFile()));
			HashMap<Long, Integer> maps = new HashMap<>();
			String line = null;
			String ordinateur = InetAddress.getLocalHost().getHostName();
			while ((line = reader.readLine()) != null) {
				long hash = Integer.toUnsignedLong(line.hashCode());
				if (!maps.containsKey(hash)) {
					maps.put(hash, 1);
					String outputFile = "/tmp/cochener/shuffles/" + hash + "-" + ordinateur + ".txt";
					File newTextFile = new File(outputFile);
					FileWriter fw = new FileWriter(newTextFile);
					fw.write(line);
					fw.close();
				} else {
					maps.put(hash, maps.get(hash) + 1);
					FileWriter fw = new FileWriter("/tmp/cochener/shuffles/" + hash + "-" + ordinateur + ".txt", true);
					fw.write("\n" + line);
					fw.close();
				}
			}

			// Envoie des fichiers sur les nouveaux ordinateurs
			for (Long hashKey : maps.keySet()) {
				long numeroMachine = hashKey % nbMachines;
				String newOrdi = machines.get((int) numeroMachine);
				String file = hashKey + "-" + ordinateur + ".txt";

				// Recherche du dossier dans /tmp/cochener
				String idNewOrdi = "cochener@" + newOrdi;
				Process proc = new ProcessBuilder().command("ssh", idNewOrdi, "ls", "/tmp/cochener").start();
				BufferedReader files = new BufferedReader(new InputStreamReader(proc.getInputStream()));
				String l = "";
				ArrayList<String> listFiles = new ArrayList<String>();
				while ((l = files.readLine()) != null) {
					listFiles.add(l);
				}
				if (!listFiles.contains("shufflesreceived")) {
					testCommand("ssh" + " cochener@" + newOrdi + " mkdir" + " -p" + " /tmp/cochener/shufflesreceived",
							20);
				}
				testCommand("ssh" + " cochener@" + ordinateur + " scp" + " /tmp/cochener/shuffles/" + file
						+ " cochener@" + newOrdi + ":/tmp/cochener/shufflesreceived/" + file, 50);
			}
		}

		if (args[0].contentEquals("2") && args.length == 1) {
			// On regarde les fichiers qui sont dans shufflesreceived
			String ordinateur = InetAddress.getLocalHost().getHostName();
			Process proc = new ProcessBuilder()
					.command("ssh", "cochener@" + ordinateur, "ls", "/tmp/cochener/shufflesreceived").start();
			BufferedReader files = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String l = "";
			ArrayList<String> listFiles = new ArrayList<String>();
			while ((l = files.readLine()) != null) {
				listFiles.add(l);
			}
			// Hashmap des hashcode du dossier
			HashMap<String, Integer> maps = new HashMap<>();
			for (int i = 0; i < listFiles.size(); i++) {
				// On récupère le hashcode
				String[] tempTab = listFiles.get(i).split("-");
				String hash = tempTab[0];
				if (!maps.containsKey(hash)) {
					maps.put(hash, 1);
				}
			}
			
			for (Entry<String, Integer> hashcode : maps.entrySet()) {
				String outputFile = "/tmp/cochener/reduces/" + hashcode.getKey() + ".txt";
				HashMap<String, Integer> reduce = new HashMap<>();
				for (int j = 0; j < listFiles.size(); j++) {
					if (listFiles.get(j).contains(hashcode.getKey())) {
						@SuppressWarnings("resource")
						BufferedReader br = new BufferedReader(
								new FileReader("/tmp/cochener/shufflesreceived/" + listFiles.get(j)));
						String line = null;
						while ((line = br.readLine()) != null) {
							String[] parts = line.split(" ", 2);
							String key = parts[0];
							Integer value = Integer.parseInt(parts[1]);
							if (!reduce.containsKey(key)) {
								reduce.put(key, value);
							} else {
								reduce.put(key, reduce.get(key) + 1);
							}
						}
					}
				}
				for (Entry<String, Integer> k : reduce.entrySet()) {
					String content = k.getKey() + " " + k.getValue();
					System.out.println(content);
					FileWriter fw = new FileWriter(outputFile, true);
					fw.write(content);
					fw.close();
				}
			}
		}
	}
}
