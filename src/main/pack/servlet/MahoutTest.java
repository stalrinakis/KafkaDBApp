package main.pack.servlet;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import main.pack.servlet.TestServ.PostEntry;

public class MahoutTest {

    public static void mahoutRun(List<PostEntry> postEntries) {
    	
    	
    	
        Map<String, String> postsToTrain = new HashMap<>();
        Map<String, String> postsToPredict = new HashMap<>();
        Map<String, String> orginialCat = new HashMap<>();

        // Calculate the number of entries for training and prediction
        int totalEntries = postEntries.size();
        int trainSize = (int) (totalEntries * 0.75); // 75% for training
        int predictSize = totalEntries - trainSize; // Remaining 25% for prediction

        // Splitting the data into training and prediction sets
        int trainCount = 0;
        int predictCount = 0;
        for (PostEntry item : postEntries) {
            if (trainCount < trainSize) {
                postsToTrain.put(item.getText(), item.getCategory());
                trainCount++;
            } else if (predictCount < predictSize) {
                postsToPredict.put(item.getId(), item.getText());
                orginialCat.put(item.getId(), item.getCategory());
                predictCount++;
            } else {
                break; // Break the loop once both maps are filled according to the desired ratio
            }
        }

        try {
            // Load the word list
            URL url = Objects.requireNonNull(MahoutTest.class.getClassLoader().getResource("main/resources/unique_words.txt"), "Resource not found.");
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
            List<String> wordList = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.split("\\s+"); // Split line into words using whitespace as delimiter
                for (String word : words) {
                    // Add word to the list
                    wordList.add(word);
                }
            }
            reader.close();

            // Initialize vocabulary and category dictionary
            Map<String, Integer> vocab = new HashMap<>();
            Map<String, Integer> catDict = new HashMap<>();
            int label = 0;

            // Preprocess training data and populate vocabulary
            for (String title : postsToTrain.keySet()) {
                String cat = postsToTrain.get(title);

                // Update category dictionary
                if (!catDict.containsKey(cat)) {
                    catDict.put(cat, label);
                    label++;
                }

                // Preprocess text and populate vocabulary
                title = preprocessText(title, wordList);
                String[] tokens = title.split("\\s+");
                for (String word : tokens) {
                    vocab.putIfAbsent(word, vocab.size());
                }
            }

            // Initialize logistic regression model weights
            int numCategories = catDict.size();
            int numFeatures = vocab.size();
            double[][] weights = new double[numCategories][numFeatures];

            // Train the logistic regression model
            for (String title : postsToTrain.keySet()) {
                String cat = postsToTrain.get(title);

                // Preprocess text and vectorize
                title = preprocessText(title, wordList);
                String[] tokens = title.split("\\s+");
                double[] vector = vectorize(tokens, vocab);

                // Update weights
                train(weights, vector, catDict.get(cat));
            }

            for (String postId : postsToPredict.keySet()) {
                String text = postsToPredict.get(postId);
                String originalCategory = orginialCat.get(postId);

                // Preprocess text and vectorize
                text = preprocessText(text, wordList);
                String[] tokens = text.split("\\s+");
                double[] vector = vectorize(tokens, vocab);

                // Use the model to make a prediction
                int predictedCategoryIndex = predict(weights, vector);

                // Convert predicted category index back to the original category
                String predictedCategory = null;
                for (Map.Entry<String, Integer> entry : catDict.entrySet()) {
                    if (entry.getValue().equals(predictedCategoryIndex)) {
                        predictedCategory = entry.getKey();
                        break;
                    }
                }

                System.out.println("Predicted category for '" + postId + "' with original category '" + originalCategory + "': " + predictedCategory);
            }



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void train(double[][] weights, double[] vector, int category) {
        for (int i = 0; i < vector.length; i++) {
            weights[category][i] += vector[i];
        }
    }

    private static int predict(double[][] weights, double[] vector) {
        double maxScore = Double.NEGATIVE_INFINITY;
        int bestCategory = -1;
        for (int i = 0; i < weights.length; i++) {
            double score = 0;
            for (int j = 0; j < vector.length; j++) {
                score += weights[i][j] * vector[j];
            }
            if (score > maxScore) {
                maxScore = score;
                bestCategory = i;
            }
        }
        return bestCategory;
    }

    private static String preprocessText(String text, List<String> wordList) {
        for (String word : wordList) {
            text = text.toLowerCase().trim().replaceAll("\\b" + word + "\\b", "").replaceAll("[^a-zA-Z0-9]", " ");
        }
        return text;
    }

    private static double[] vectorize(String[] tokens, Map<String, Integer> vocab) {
        double[] vector = new double[vocab.size()];
        for (String token : tokens) {
            Integer index = vocab.get(token);
            if (index != null) {
                vector[index]++;
            }
        }
        return vector;
    }
}
