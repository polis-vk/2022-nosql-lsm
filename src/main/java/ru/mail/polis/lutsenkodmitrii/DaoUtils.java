package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static ru.mail.polis.lutsenkodmitrii.PersistenceRangeDao.NEXT_LINE_IN_STRING;
import static ru.mail.polis.lutsenkodmitrii.PersistenceRangeDao.NEXT_LINE_REPLACEMENT;

public final class DaoUtils {

    public static final int CHARS_IN_INT = Integer.SIZE / Character.SIZE;

    private DaoUtils() {
    }

    public static void writeUnsignedInt(int k, BufferedWriter bufferedWriter) throws IOException {
        bufferedWriter.write((k + '0') >>> 16);
        bufferedWriter.write((k + '0'));
    }

    public static int readUnsignedInt(BufferedReader bufferedReader) throws IOException {
        int ch1 = bufferedReader.read();
        int ch2 = bufferedReader.read();
        // Для чтения везде используется BufferedReader.
        // Все его методы возвращают -1, когда достигнут конец файла
        // Для поддержания идентичного контракта и в этом методе возвращается -1 если достигнут конец файла
        if (ch1 == -1 || ch2 == -1) {
            return -1;
        }
        return (ch1 << 16) + ch2 - '0';
    }

    public static String readKey(BufferedReader bufferedReader) throws IOException {
        int keyLength = readUnsignedInt(bufferedReader);
        if (keyLength == -1) {
            return null;
        }
        char[] keyChars = new char[keyLength];
        bufferedReader.read(keyChars);
        return postprocess(new String(keyChars));
    }

    public static String readValue(BufferedReader bufferedReader) throws IOException {
        return bufferedReader.read() == PersistenceRangeDao.EXISTING_MARK
                ? postprocess(bufferedReader.readLine())
                : null;
    }

    public static BaseEntry<String> readEntry(BufferedReader bufferedReader) throws IOException {
        bufferedReader.skip(CHARS_IN_INT); // Пропускаем длину предыдущей записи
        String key = readKey(bufferedReader);
        if (key == null) {
            // Таким образом выполняется проверка конца файла, поэтому не кидается EOFExсeption
            // Более изящный способ проверить конец файла для BufferedReader я не нашел/придумал
            // На skip, который выше нельзя проверять так как там идет запись длины предыдущей записи
            // И на последней записи можно успешно сделать skip, но ключа уже не будет
            // Можно для последней записи не записывать длину, но код тогда будет некрасивый
            // Или надо будет записать и сразу удалять эту длину, от чего я тоже отказался
            return null;
        }
        return new BaseEntry<>(key, readValue(bufferedReader));
    }

    /**
     * Все размер / длины ы в количественном измерении относительно char, то есть int это 2 char
     * Везде, где упоминается размер / длина, имеется в виду относительно char, а не байтов.
     * left - левая граница, равная offset - сдвиг относительно начала файла
     * offset нужен, чтобы пропустить к примеру минимальный и максимальный ключи в начале (См. Описание формата файла)
     * right - правая граница равная размеру файла минус размер числа,
     * которое означает длину относящегося предыдущей записи
     * Минусуем, чтобы гарантированно читать это число целиком.
     * position - середина по размеру между left и right, (left + right) / 2;
     * position после операции выше указывает на ту позицию относительно начала строки, на какую повезет,
     * необязательно на начало
     * Поэтому реализован гарантированный переход на начало следующей строки, для этого делается readline,
     * Каждое entry начинается с новой строки ('\n' в исходном ключе и значении экранируется)
     * Начало строки начинается всегда с размера прошлой строки, то есть прошлой entry
     * плюс размера одного int(этого же число, но на прошлой строке)
     * При этом left всегда указывает на начало строки, а right на конец (речь про разные строки / entry)
     * Перед тем как переходить на position в середину, всегда ставиться метка в позиции left, то есть в начале строки
     * Всегда идет проверка на случай если мы пополи на середину последней строки :
     * position + readBytes(прочитанные байты с помощью readline) == right - offset,
     * вычитаем offset так как right относительно всего размера файла,
     * Если равенство выполняется, то возвращаемся в конец последней строки -
     * position ставим в left + CHARS_IN_INT (длина числа, размера предыдущей строки), readBytes обнуляем,
     * дальше идет обычная обработка :
     * Читаем ключ, значение (все равно придется его читать чтобы дойти до след ключа),
     * сравниваем ключ, если он равен, то return
     * Если текущий ключ меньше заданного, то читаем следующий
     * Если следующего нет, то return null, так ищем границу сверху, а последний ключ меньше заданного
     * Если этот следующий ключ меньше или равен заданному, то читаем его value и return
     * В зависимости от результата сравнения left и right устанавливаем в начало или конец рассматриваемого ключа
     * mark делается всегда в начале entry то есть в позиции left. В первом случае чтобы вернуться, как бы skip наоборот
     * Во втором чтобы сделать peek для nextKey и была возможность просмотреть его и вернуться в его начало.
     * В итоге идея следующая найти пару ключей, между которыми лежит исходный и вернуть второй или равный исходному,
     * при этом не храня индексы для сдвигов ключей вовсе
     */
    public static BaseEntry<String> ceilKey(Path path, BufferedReader bufferedReader,
                                            String key, long offset) throws IOException {
        int prevEntryLength;
        String currentKey;
        long left = offset;
        long right = Files.size(path) - CHARS_IN_INT;
        long position;
        while (left <= right) {
            position = (left + right) / 2;
            bufferedReader.mark((int) right);
            bufferedReader.skip(position - left);
            String leastPartOfLine = bufferedReader.readLine();
            int readBytes = leastPartOfLine.length() + 1;
            prevEntryLength = readUnsignedInt(bufferedReader);
            if (position + readBytes >= right) {
                bufferedReader.reset();
                bufferedReader.skip(CHARS_IN_INT);
                right = position - prevEntryLength + readBytes + 1;
                position = left + CHARS_IN_INT;
                readBytes = 0;
            }
            currentKey = readKey(bufferedReader);
            String currentValue = readValue(bufferedReader);
            int compareResult = key.compareTo(currentKey);
            if (compareResult == 0) {
                return new BaseEntry<>(currentKey, currentValue);
            }
            if (compareResult > 0) {
                bufferedReader.mark(0);
                prevEntryLength = readUnsignedInt(bufferedReader);
                String nextKey = readKey(bufferedReader);
                if (nextKey == null) {
                    return null;
                }
                if (key.compareTo(nextKey) <= 0) {
                    return new BaseEntry<>(nextKey, readValue(bufferedReader));
                }
                left = position + readBytes + prevEntryLength;
                bufferedReader.reset();
            } else {
                right = position + readBytes;
                bufferedReader.reset();
            }
        }
        return null;
    }

    public static String preprocess(String str) {
        return str.replace(NEXT_LINE_IN_STRING, NEXT_LINE_REPLACEMENT);
    }

    public static String postprocess(String str) {
        return str.replace(NEXT_LINE_REPLACEMENT, NEXT_LINE_IN_STRING);
    }
}