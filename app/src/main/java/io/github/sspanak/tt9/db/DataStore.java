package io.github.sspanak.tt9.db;

import android.content.Context;
import android.os.CancellationSignal;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.github.sspanak.tt9.db.entities.AddWordResult;
import io.github.sspanak.tt9.db.entities.CustomWord;
import io.github.sspanak.tt9.db.wordPairs.WordPairStore;
import io.github.sspanak.tt9.db.words.WordStore;
import io.github.sspanak.tt9.languages.Language;
import io.github.sspanak.tt9.preferences.settings.SettingsStore;
import io.github.sspanak.tt9.util.Logger;
import io.github.sspanak.tt9.util.SupremeExecutor;

public class DataStore {
	private final static String LOG_TAG = DataStore.class.getSimpleName();

	private static ExecutorService executor;
	private static ScheduledExecutorService timeoutExecutor;

	@Nullable private static Future<?> getWordsTask;
	@Nullable private static volatile CancellationSignal getWordsCancellationSignal;
	private static volatile int getWordsGeneration = 0;

	private static WordPairStore pairs;
	private static WordStore words;


	public static void init(@NonNull Context context) {
		executor = executor == null ? SupremeExecutor.get() : executor;
		timeoutExecutor = timeoutExecutor == null || timeoutExecutor.isShutdown() || timeoutExecutor.isTerminated() ? Executors.newSingleThreadScheduledExecutor((runnable) -> {
			Thread thread = new Thread(runnable, "TT9-getWords-timeout");
			thread.setDaemon(true);
			return thread;
		}) : timeoutExecutor;
		pairs = pairs == null ? new WordPairStore(context.getApplicationContext()) : pairs;
		words = words == null ? new WordStore(context.getApplicationContext()) : words;
	}


	@Nullable
	private static Future<?> runInThread(@NonNull Runnable action) {
		if (executor == null || executor.isShutdown() || executor.isTerminated()) {
			Logger.e(LOG_TAG, "Cannot run a DataStore task when the executor is shutdown or NULL.");
			return null;
		}

		try {
			return executor.submit(() -> {
				try {
					action.run();
				} catch (Exception e) {
					Logger.ex(LOG_TAG, "Error in DataStore task.", e);
				}
			});
		} catch (RejectedExecutionException e) {
			Logger.e(LOG_TAG, "Failed running async DataStore task. " + e);
			return null;
		}
	}


	private static void runInTransaction(@NonNull Runnable action, @NonNull Runnable onFinish, @NonNull String errorMessagePrefix) {
		runInThread(() -> {
			try {
				words.startTransaction();
				action.run();
				words.finishTransaction();
			} catch (Exception e) {
				words.failTransaction();
				Logger.e(LOG_TAG, errorMessagePrefix + " " + e.getMessage());
			}
			onFinish.run();
		});
	}


	public static void normalizeNext() {
		words.normalizeNext();
	}


	public static void getLastLanguageUpdateTime(Consumer<String> notification, Language language) {
		runInThread(() -> notification.accept(words.getLanguageFileHash(language)));
	}


	public static void deleteCustomWord(Runnable notification, Language language, String word) {
		runInThread(() -> {
			words.removeCustomWord(language, word);
			notification.run();
		});
	}


	public static void put(Consumer<AddWordResult> statusHandler, Language language, String word) {
		runInThread(() -> statusHandler.accept(words.put(language, word)));
	}


	public static void makeTopWord(@NonNull Language language, @NonNull String word, @NonNull String sequence) {
		runInThread(() -> words.makeTopWord(language, word, sequence));
	}


	public static synchronized void getWords(Consumer<ArrayList<String>> dataHandler, Language language, String sequence, boolean onlyExactSequence, String filter, boolean orderByLength, int minWords, int maxWords) {
		if (getWordsTask != null && !getWordsTask.isDone()) {
			CancellationSignal cancellationSignal = getWordsCancellationSignal;
			if (cancellationSignal != null) {
				cancellationSignal.cancel();
			}
		}

		final int generation = ++getWordsGeneration;
		final CancellationSignal cancellationSignal = new CancellationSignal();
		getWordsCancellationSignal = cancellationSignal;
		getWordsTask = runInThread(() -> getWordsSync(dataHandler, language, sequence, onlyExactSequence, filter, orderByLength, minWords, maxWords, cancellationSignal, generation));
		setGetWordsTimeout(getWordsTask, cancellationSignal, generation);
	}


	private static void getWordsSync(Consumer<ArrayList<String>> dataHandler, Language language, String sequence, boolean onlyExactSequence, String filter, boolean orderByLength, int minWords, int maxWords, @NonNull CancellationSignal cancellationSignal, int generation) {
		try {
			ArrayList<String> data = words.getMany(cancellationSignal, language, sequence, onlyExactSequence, filter, orderByLength, minWords, maxWords);
			if (isLatestGetWordsRequest(cancellationSignal, generation)) {
				dataHandler.accept(data);
			}
		} catch (Exception e) {
			if (isLatestGetWordsRequest(cancellationSignal, generation)) {
				Logger.e(LOG_TAG, "Error fetching words: " + e.getMessage());
			}
		}
	}


	private static void setGetWordsTimeout(@Nullable Future<?> task, @NonNull CancellationSignal cancellationSignal, int generation) {
		if (task == null || timeoutExecutor == null || timeoutExecutor.isShutdown() || timeoutExecutor.isTerminated()) {
			return;
		}

		timeoutExecutor.schedule(() -> {
			if (!task.isDone() && isLatestGetWordsRequest(cancellationSignal, generation)) {
				cancellationSignal.cancel();
				Logger.e(LOG_TAG, "Word loading timed out after " + SettingsStore.SLOW_QUERY_TIMEOUT + " ms.");
			}
		}, SettingsStore.SLOW_QUERY_TIMEOUT, TimeUnit.MILLISECONDS);
	}


	private static boolean isLatestGetWordsRequest(@NonNull CancellationSignal cancellationSignal, int generation) {
		if (cancellationSignal.isCanceled() || generation != getWordsGeneration) {
			return false;
		}

		return cancellationSignal == getWordsCancellationSignal;
	}


	public static void getCustomWords(Consumer<ArrayList<CustomWord>> dataHandler, String wordFilter, int maxWords) {
		runInThread(() -> dataHandler.accept(words.getSimilarCustom(wordFilter, maxWords)));
	}


	public static void countCustomWords(Consumer<Long> dataHandler) {
		runInThread(() -> dataHandler.accept(words.countCustom()));
	}


	public static void exists(Consumer<ArrayList<Integer>> dataHandler, ArrayList<Language> languages) {
		runInThread(() -> dataHandler.accept(words.exists(languages)));
	}


	@Nullable
	public static String getWord(@NonNull Language language, @NonNull String word, @NonNull String sequence) {
		return words.getWord(language, word, sequence);
	}


	public static void addWordPair(Language language, String word1, String word2, String sequence2) {
		pairs.add(language, word1, word2, sequence2);
	}


	public static String getWord2(Language language, String word1, String sequence2) {
		return pairs.getWord2(language, word1, sequence2);
	}


	public static void saveWordPairs() {
		pairs.save();
	}


	public static void loadWordPairs(ArrayList<Language> languages) {
		runInThread(() -> pairs.load(languages));
	}


	public static void clearWordPairCache() {
		pairs.clearCache();
	}


	public static void deleteWordPairs(@NonNull ArrayList<Language> languages, @NonNull Runnable onDeleted) {
		runInTransaction(() -> pairs.remove(languages), onDeleted, "Failed deleting word pairs.");
	}


	public static String getWordPairStats() {
		return pairs.toString();
	}
}
