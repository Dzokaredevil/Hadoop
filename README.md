# Hadoop

Задание для курса "Современные информационные системы".

Задания выполнил **Войтович Дмитрий**.

Все лежит в [этом](https://github.com/Dzokaredevil/Hadoop) репозитории.
На хосте же все лежит в папке `Dzokaredevil`. Структура точно такая же, как и в репозитории.

URL дев хоста: `ant.apmath.spbu.ru`.

---

# Задания по Hadoop

Задания строятся на основе парадигмы `Map Reduce`.

## Задание 2.1

> Перепишите программу, подсчитывающую частоту использования слов в тексте, используя библиотеку Lucene: учтите все возможные разделители слов и исключите стоп-слова из вывода. Для этого воспользуйтесь стандартным анализатором.

Задание лежит в папке `2point1/src`


## Задание 2.2

> Измените программу из задания 1, так чтобы посчитать количество слов определенной длины в тексте. Результатом работы программы должна стать таблица вида {длина слова → количество слов такой длины}. Ключ должен иметь интегральный тип.

Задание лежит в папке `2point2/src`


## Задание 2.3

> Измените программу из задания 1, так чтобы для каждого слова вывести слово, чаще всего следующее за ним. Учтите знаки препинания, являющиеся разделителями для предложений, чтобы исключить из вывода слова, идущие друг за другом, но находящиеся в разных предложениях.

Задание лежит в папке `2point3/src`


## Задание 2.4

> Измените программу из предыдущего задания так, чтобы дополнить вывод частотой встречи слов (количество раз, которое слово-значение следует за словом-ключом). Для этого создайте новый выходной тип с соответствующими полями, реализовав интерфейс org.apache.hadoop.io.Writable.

Задание лежит в папке `2point4/src`


## Задание 2.5

> Измените программу из предыдущего задания так, чтобы упорядочить вывод по убыванию частоты встречи слов. Для этого необходимо реализовать интерфейс `org.apache.hadoop.io.WritableComparable` у созданного вами типа и дать планировщику дополнительное задание, которое отсортирует данные.

Задание лежит в папке `2point5/src`


## Задание 3

> Произведите предварительную обработку данных NDBC, которые представляют собой записи частотно-направленных спектров морского волнения, полученных со специальных исследовательских буев и закодированных в пяти переменных.


Задание лежит в папке `3/src`

---


# Задание 5. Обработка данных с помощью функциональных примитивов (Spark)

Задания по `Spark`

## Задание 5.1

> Найдите самую часто встречающуюся пару идущих друг за другом слов в тексте. Каталог с заготовкой программы: `/mnt/root/hadoop/04-spark-wc`

Задание лежит в папке `5point1/src`


## Задание 5.2

> Решите задачу с предобработкой спектров на Spark без использования возможностей Hadoop (реализованный формат данных и др.).

>Для работы с именами файлов используйте стандартный класс java.io.File и тот факт, что файлы параллельной файловой системы доступны как обычные файлы с префиксом `/mnt/root`. Проверить работу программы можно с помощью команды `./bin/check-spec`

> Каталог с заготовкой программы: `/mnt/root/hadoop/05-spark-spec`

Задание лежит в папке `5point2/src`

---

## Сборка кода

```bash
$ cd /home/Dzokaredevil/Hw
$ cd ${нужное задание}/src
$ ant
$ ./run
```


## Логи output

Логи все можно найти в `/mnt/root/home/Dzokaredevil/output-z${номер задачи}`