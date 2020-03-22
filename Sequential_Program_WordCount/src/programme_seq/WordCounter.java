package programme_seq;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.*;


public class WordCounter {
	
	// Etape 1
	@SuppressWarnings("resource")
	public static HashMap<String,Integer> countWords(String filename) throws IOException {
		// Lecture du fichier texte
		BufferedReader reader = new BufferedReader(new FileReader(Paths.get(filename).toFile()));
		// Création de la table de hachage vide qui contiendra les mots et leur nombre d'occurrences
		HashMap<String, Integer> counts = new HashMap<>();
		String line = null;
		while((line = reader.readLine()) != null) {
			// Pour chaque mot dans la ligne 
			for (String word : line.split(" ")) {
				// Si le mot est vide, le programme passe au mot suivant
				if (word.isEmpty()) {
					continue;
				}
				// Si la table de hachage ne contient pas le mot
				if (!counts.containsKey(word)) {
					// Ajout du mot dans la table et attribution de la valeur 1
					counts.put(word, 1);
				} else {
					// Sinon le compteur du mot est incrémenté de 1
					counts.put(word, counts.get(word)+1);
				}
			}
		}
		// Sortie : la table de hachage remplie avec les mots du texte associés à leur nombre d'occurrences
		return counts;
	}

	// Etape 2
	public static LinkedHashMap<String,Integer> sortByValue(String filename) throws IOException {
		LinkedHashMap<String,Integer> sortedMap = countWords(filename)
				// Retourner une collection d'entrées de map
				.entrySet()
				// Traiter la map comme une séquence d'éléments
				.stream() 
				// Trier les objets selon la fonction reverseOrder
				.sorted(Collections.reverseOrder(HashMap.Entry.comparingByValue())) 
				// Collecter les éléments trier dans une nouvelle LinkedHashMap
				.collect(toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));
		return sortedMap;
	}
	
	
	// Etape 3
	public static LinkedHashMap<String,Integer> sortByKey(String filename) throws IOException { 
		long startTime = System.currentTimeMillis();
		HashMap<String,Integer> input = countWords(filename);
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
        System.out.println("Running time 'Count Words' : " + totalTime);
		long startTime2 = System.currentTimeMillis();
		LinkedHashMap<String,Integer> sortedInput = new LinkedHashMap<>();
		sortedInput = input
				.entrySet()
				.stream()
				.sorted(Collections.reverseOrder(HashMap.Entry.<String, Integer>comparingByValue())
								   .thenComparing(HashMap.Entry.comparingByKey()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
		long endTime2   = System.currentTimeMillis();
		long totalTime2 = endTime2 - startTime2;
	    System.out.println("Running time 'Sort' : " + totalTime2);
		return sortedInput;
	}
	
		
	public static void main(String[] args) throws IOException {
		LinkedHashMap<String,Integer> res = sortByKey("CC-MAIN-20170322212949-00140-ip-10-233-31-227.ec2.internal.warc.wet");

        int i = 0;
        for(HashMap.Entry<String, Integer> mapping : res.entrySet()){
        	if (i > 50) break;
        	System.out.println(mapping.getKey() + " " + mapping.getValue());
        	i += 1;
        } 
	}
}
