package id.unimi.di.abd;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.stream.IntStream;

public class App {
    public static void main(String[] args) throws InterruptedException, IOException {
/*        SubmissionPublisher<String> publisher = new SubmissionPublisher<>();

        publisher.subscribe(new StringSubsriber());

        IntStream.range(0,10)
                .mapToObj(e -> String.format("Ciao %d",e))
                .forEach(publisher::submit);
        publisher.close();
        Thread.sleep(1000);*/
        FileReader fileReader = new FileReader("./prova.txt");
        FileWriter f = new FileWriter("./prova.txt");
    }

    private static class StringSubsriber<String> implements Flow.Subscriber<String> {
        private Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(String item) {
            System.out.println(item);
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println("Errore");
        }

        @Override
        public void onComplete() {
            System.out.println("DONE");
        }
    }
}
