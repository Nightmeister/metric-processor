package net.aivanov;

import net.aivanov.metric.processor.impl.NaiveMetricProcessorImpl;

public class Application {

    public static void main(String[] args) {
        NaiveMetricProcessorImpl metricProcessor = new NaiveMetricProcessorImpl("/Users/artyom.ivanov/Documents/Test/store");

        metricProcessor.add(0, 'a', 1);
        metricProcessor.add(1, 'b', 1);
        metricProcessor.add(2, 'a', -3);

        System.out.println(metricProcessor.sum(0, 3, 'a')); // -2
        System.out.println(metricProcessor.sum(2, 3, 'b')); // 0
        System.out.println(metricProcessor.sum(0, 1, 'b')); // 0

        /*
         * //Если реализовать требуемый класс, то следующий код:
         * add(0, 'a', 1);
         * add(1, 'b', 1);
         * add(2, 'a', -3);
         * System.out.println(sum(0, 3, 'a'));
         * System.out.println(sum(2, 3, 'b'));
         * System.out.println(sum(0, 1, 'b'));
         *
         * //Должен будет вывести в консоль:
         * -2
         * 1 Ошибка? timestamp у b = 1, запрашиваемый range 2 - 3. Почему 1?
         * 0
         */
    }
}
