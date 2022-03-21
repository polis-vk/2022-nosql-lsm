package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class DaoUtils {

    public static final int CHARS_IN_INT = Integer.SIZE / Character.SIZE;

    private DaoUtils() {
    }

    public static void writeUnsignedInt(int k, BufferedWriter bufferedWriter) throws IOException {
        // Почему здесь +1 См. метод ниже - readUnsignedInt(BufferedReader bufferedReader)
        bufferedWriter.write((char) ((k + 1) >>> 16));
        bufferedWriter.write((char) (k + 1));
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
        // Здесь -1 делается по следующей причине:
        // В MusicTest все ключи имеют длину 13, кроме одного, с длинной 14
        // Если не добавлять в методе выше (writeUnsignedInt) 1 при записи, там k + 1 везде, то:
        // При записи в файл длины ключа для "Ar1", "Al12", "T1111" длины 14 в файл его длина записывается как бы 2 раза
        // С остальными ключамии (длина 13) все нормально
        // Однако, если при записи добавить 1, как следствии в строке ниже надо вычитать её, то все нормально
        // То есть на месте ключей длины 13 будет 14, а при 14 будет 15, это в файле
        // При чтении 1 вычитается
        // Я подумад сначала что может быть при char 14 какой-то особый символ или еще что-то такое
        // Но ведь добавив 1 все ключи 13 записываются как 14 и записываеются нормально.
        // Получается если записывать 13 + 1 (+1 внутри метода writeUnsignedInt) , то все нормально
        // А если 14 без добавления +1 то не нормально
        // Почему так мне не понятно, единственна догадка - приведения типов или что-то подобное, вызываемое этой 1
        return (ch1 << 16) + (ch2) - 1;
    }

    public static String readKey(BufferedReader bufferedReader) throws IOException {
        int keyLength = readUnsignedInt(bufferedReader);
        if (keyLength == -1) {
            return null;
        }
        char[] keyChars = new char[keyLength];
        bufferedReader.read(keyChars);
        return new String(keyChars);
    }

    public static BaseEntry<String> readEntry(BufferedReader bufferedReader) throws IOException {
        bufferedReader.skip(CHARS_IN_INT); // Пропускаем длину предыдущей записи и всего что к ней относиться в байтах
        int keyLength = readUnsignedInt(bufferedReader);
        // Для чтения везде используется BufferedReader.
        // Все его методы возвращают -1, когда достигнут конец файла
        // Для поддержания идентичного контракта в методе readUnsignedInt возвращается -1 если достигнут конец файла
        // И как следствие возвращается null в текущем методе
        // Таким образом выполняется и проверка конца файла, поэтому не кидается EOFExсeption
        // Более изящный способ проверить конец файла для BufferedReader я не нашел/придумал
        // На skip, который выше нельзя проверять так как там идет запись длины предыдущей записи
        // И на последней записи можно успешно сделать skip, но ключа уже не будет
        // Можно для последней записи не записывать длину, но код тогда будет некрасивый
        // Или надо будет записать и сразу удалять эту длину, от чего я тоже отказался
        if (keyLength == -1) {
            return null;
        }
        char[] keyChars = new char[keyLength];
        bufferedReader.read(keyChars);
        return new BaseEntry<>(new String(keyChars), bufferedReader.readLine());
    }

    /**
     * Все размер / длины ы в количественном измерении относительно char, то есть int это 2 char
     * Везде, где упоминается размер / длина, имеется в виду относительно char, а не байтов.
     * left - левая граница, равная offset - сдвиг относительно начала файла
     * offset нужен, чтобы пропустить к примеру минимальный и максимальный ключи в начале (См. Описание формата файла)
     * right - правая граница равная размеру файла минус размер числа, которое означает длину относящегося предыдущей записи
     * Минусуем, чтобы гарантированно читать это число целиком.
     * mid - середина по размеру между left и right, (left + right) / 2;
     * mid указывает на ту позицию относительно начала строки, на какую повезет, необязательно на начало
     * Но реализован гарантированный переход на начало следующей строки, для этого делается readline,
     * Каждое entry начинается с новой строки ('\n' в исходном ключе / значении экранируется)
     * Начало строки начинается всегда с размера прошлой строки, то есть прошлой entry
     * плюс размера одного int(этого же число, но напрошлой строке)
     * При этом left всегда указывает на начало строки, а right на конец (речь про разные строки / entry)
     * Перед тем как переходить на mid всегда ставиться метка в позиции left, то есть в начале строки
     * Всегда идет проверка на случай если мы пополи на середину последней строки :
     * mid + прочитанные байты за readline == right - offset,
     * вычитаем offset так как right относительно всего размера файла,
     * Если равенство выполняется, то возвращаемся в конец последней строки и идет обычная обработка :
     * Читаем ключ, значение (все равно придется его читать чтобы дойти до след ключа),
     * сравниваем ключ, если он равен, то return
     * Если текущий ключ меньше заданного, то читаем следующий
     * Если следующего нет, то return null, так ищем границу сверху, а последний ключ меньше заданного
     * Если этот следующий ключ меньше или равен заданному, то читаем его value и return
     * В зависимости от результата сравнения left и right устанавливаем в начало или конец рассматриваемого ключа
     * В итоге идея следующая найти пару ключей, между которыми лежит исходный и вернуть второй или равный исходному
     */
    public static BaseEntry<String> ceilKey(Path path, BufferedReader bufferedReader,
                                            String key, long offset) throws IOException {

        int prevEntryLength;
        String currentKey;
        long left = offset;
        long right = Files.size(path) - CHARS_IN_INT;
        long mid;
        while (left <= right) {
            mid = (left + right) / 2;
            bufferedReader.mark((int) right);
            bufferedReader.skip(mid - left);
            int readBytes = bufferedReader.readLine().length() + 1;
            prevEntryLength = readUnsignedInt(bufferedReader);
            if (mid + readBytes >= right - offset) {
                bufferedReader.reset();
                bufferedReader.skip(CHARS_IN_INT);
                right = mid - prevEntryLength + readBytes + 1;
            }
            currentKey = readKey(bufferedReader);
            String currentValue = bufferedReader.readLine();
            int compareResult = key.compareTo(currentKey);
            if (compareResult == 0) {
                return new BaseEntry<>(currentKey, currentValue);
            }
            if (compareResult > 0) {
                bufferedReader.mark(0);
                bufferedReader.skip(CHARS_IN_INT);
                String nextKey = readKey(bufferedReader);
                if (nextKey == null) {
                    return null;
                }
                if (key.compareTo(nextKey) <= 0) {
                    return new BaseEntry<>(nextKey, bufferedReader.readLine());
                }
                left = mid - prevEntryLength + readBytes;
                bufferedReader.reset();
            } else {
                right = mid + readBytes;
                bufferedReader.reset();
            }
        }
        return null;
    }
}