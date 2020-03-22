package hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class masterNode {

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

	public static void main(String[] args) throws IOException, InterruptedException {
		
		cleanClass.main(null);
		deployClass.main(null);

		// On supprimer le fichier splits dans tmp pour en créer un nouveau à chaque
		// fois
		
		@SuppressWarnings("unused")
		Process p2 = new ProcessBuilder().command("mkdir", "/tmp/cochener/splits").start();

		// Lecture du fichier d'entrée
		@SuppressWarnings("resource")
		BufferedReader rd = new BufferedReader(new FileReader(Paths.get("domaine_public_fluvial.txt").toFile()));
		String s = null;
		int numFile = 0;
		// On lit chaque ligne, et tant que chaque ligne n'est pas nulle
		while ((s = rd.readLine()) != null) { 
			// Si l'élément s n'est pas vide, alors on écrit le fichier (la ligne)
			if (!s.isEmpty()) {
				FileWriter fw = new FileWriter("/tmp/cochener/splits/S" + numFile + ".txt", true);
				fw.write(s);
				fw.close();
				numFile += 1;
			}
		}

		// Lecture des fichiers dans le dossier splits local
		Process proc = new ProcessBuilder().command("ls", "/tmp/cochener/splits").start();
		BufferedReader files = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String l = "";
		ArrayList<String> listFiles = new ArrayList<String>();
		while ((l = files.readLine()) != null) {
			listFiles.add(l);
		}

		// Liste des machines connectées
		String filename = "machines.txt";
		ArrayList<String> connectedComputer = new ArrayList<String>();
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader(Paths.get(filename).toFile()));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String commande = "ssh" + " cochener@" + line + " 'hostname'";
			boolean res = testCommand(commande, 3);
			if (res) {
				connectedComputer.add(line);
				// Envoie du fichier machines.txt sur la machine connectée
				testCommand("scp" + " /tmp/cochener/machines.txt" + " cochener@" + line + ":/tmp/cochener/", 120);
			}
		}

		long startMap = System.currentTimeMillis();

		// MAP
		int i = 0;
		int j = 0;
		for (String file : listFiles) {
			String machine = connectedComputer.get(i);
			// Etape 1 : Déploiement des fichiers du split
			boolean waitDirSplit = testCommand(
					"ssh" + " cochener@" + machine + " mkdir" + " -p" + " /tmp/cochener/splits", 120);
			if (waitDirSplit) {
				testCommand("scp" + " /tmp/cochener/splits/" + file + " cochener@" + machine + ":/tmp/cochener/splits/"
						+ file, 120);
			} else {
				System.out.println("Something wrong when splitting txt files");
				break;
			}

			// Etape 2 : Lancement du slave.jar et écriture des fichiers UMx.txt (MAPS)
			boolean waitDirMaps = testCommand("ssh" + " cochener@" + machine + " mkdir" + " -p" + " /tmp/cochener/maps",
					120);
			String fileIndice = Integer.toString(j);
			if (waitDirMaps) {
				testCommand("ssh" + " cochener@" + machine + " java" + " -jar" + " /tmp/cochener/slave.jar" + " 0 "
						+ fileIndice + " /tmp/cochener/splits/" + file, 120);
			} else {
				System.out.println("Something wrong when mapping");
				break;
			}
			i += 1;
			System.out.println(j);
			j += 1;

			if (i > connectedComputer.size() - 1) {
				i = 0;
			}
		}
		System.out.println("MAP FINISHED");

		long endMap = System.currentTimeMillis();
		long durationMap = endMap - startMap;
		System.out.println("Running time 'MAP' : " + durationMap);

		// SHUFFLE
		long startShuffle = System.currentTimeMillis();

		i = 0;
		for (j = 0; j<=listFiles.size()-1; j++) {
			String machine = connectedComputer.get(i);

			// Etape 3 : Shuffle
			boolean waitDirShuffles = testCommand(
					"ssh" + " cochener@" + machine + " mkdir" + " -p" + " /tmp/cochener/shuffles", 120);
			if (waitDirShuffles) {
				String fileIndice = Integer.toString(j);
				testCommand("ssh" + " cochener@" + machine + " java" + " -jar" + " /tmp/cochener/slave.jar" + " 1 "
						+ fileIndice + " /tmp/cochener/maps/UM" + fileIndice + ".txt", 120);
			} else {
				System.out.println("Something wrong when shuffling");
				break;
			}
			i += 1;
			if (i > connectedComputer.size() - 1) {
				i = 0;
			}
		}
		System.out.println("SHUFFLE FINISHED");
		long endShuffle = System.currentTimeMillis();
		long durationShuffle = endShuffle - startShuffle;
		System.out.println("Running time 'SHUFFLE' : " + durationShuffle);

		// REDUCE
		long startReduce = System.currentTimeMillis();

		i = 0;
		while (i <= connectedComputer.size() - 1) {
			String machine = connectedComputer.get(i);
			// Etape 4 : Reduce
			boolean waitDirReduces = testCommand(
					"ssh" + " cochener@" + machine + " mkdir" + " -p" + " /tmp/cochener/reduces", 120);
			if (waitDirReduces) {
				testCommand("ssh" + " cochener@" + machine + " java" + " -jar" + " /tmp/cochener/slave.jar" + " 2",
						120);
			} else {
				System.out.println("Something wrong when reducing");
				break;
			}
			i += 1;
		}
		System.out.println("REDUCE FINISHED");
		long endReduce = System.currentTimeMillis();
		long durationReduce = endReduce - startReduce;
		System.out.println("Running time 'REDUCE' : " + durationReduce);

	}
}
