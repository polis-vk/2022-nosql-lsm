# 2022-nosql-lsm
Проект [курса](https://polis.vk.company/curriculum/program/discipline/1356/) "NoSQL" в [Образовательном центре VK в Политехе](https://polis.vk.company/).

## Этап 1. In-memory (deadline 2022-03-02 23:59:59 MSK)
### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2022-nosql-lsm.git
Cloning into '2022-nosql-lsm'...
...
$ git remote add upstream git@github.com:polis-vk/2022-nosql-lsm.git
$ git fetch upstream
From github.com:polis-vk/2022-nosql-lsm
 * [new branch]      main     -> upstream/main
```

### Make
Так можно запустить тесты (ровно то, что делает CI):
```
$ ./gradlew clean build
```

### Develop
Откройте в IDE -- [IntelliJ IDEA Community Edition](https://www.jetbrains.com/idea/) нам будет достаточно.

**ВНИМАНИЕ!** При запуске тестов или сервера в IDE необходимо передавать Java опцию `-Xmx64m`.

Сделать имплементацию интерфейса DAO, заставив пройти все тесты.
Для этого достаточно реализовать две операции: get и upsert, при этом достаточно реализации "в памяти".

Продолжайте запускать тесты и исправлять ошибки, не забывая [подтягивать новые тесты и фиксы из `upstream`](https://help.github.com/articles/syncing-a-fork/). Если заметите ошибку в `upstream`, заводите баг и присылайте pull request ;)

### Report
Когда всё будет готово, присылайте pull request в ветку `main` со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!

## Этап 2. Persistence (deadline 2022-03-09 23:59:59 MSK)
Приведите код в состояние, удовлетворяющее новым тестам. А именно: при конструировании DAO следует восстановить состояние, персистентно сохраненное в методе `close()`.
Нужно реализовать метод `get(key)`, который будет возвращать `Entry`, сохраненной в памяти или в хипе.

В `DaoFactory.Factory` появился конструктор `createDao(Config config)`, который нужно переопределить в своей реализации.
`Config` Содержит в себе `basePath` - директория для сохранения состояния DAO.

В новых тестах не предполагается использование полноценной реализации метода `get(from, to)`. 

Следует иметь в виду, что в дальнейшем в этом файле будет происходить бинарный поиск без полной загрузки в память (но на данном этапе, это можно игнорировать).

### Report
Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!

## Этап 3. Merge Iterator (deadline 2022-03-23 23:59:59 MSK)
Приведите код в состояние, удовлетворяющее данным запросам:
* Бинарный поиск по файлу теперь является обязательным для реализации.
* Метод `get(from, to)` должен работать полноценно и полностью поддерживаться DAO даже при наличии в ней данных, записанных в файл.
* Стоит учитывать, что в диапазоне от `from` до `to`, некая часть данных может уже находиться в файле на диске, в то время, как другая часть будет все еще `InMemory`. Несмотря на это, необходимо уметь последовательно выдавать все данные в правильно отсортированном виде, реализовав итератор, способный выдавать следующее по порядку `entry`, анализируя информацию сразу из многих источников(все созданные на данный момент файлы + `InMemory` часть).
* Запрошенный диапазон значений может быть слишком велик, чтобы держать его полностью в памяти, нужно динамически подгружать лишь необходимую в данный момент часть этого диапазона.
* За один тест метод `close` у конкретного инстанса DAO может быть вызван только один раз, однако после этого имеется возможность создать новый инстанс с тем же `config`(т.е будет произведено переоткрытие DAO). В отличие от прошлого этапа, в этот раз в DAO можно будет писать новую информацию, а не только читать старую из него. Количество таких переоткрытий за один тест неограниченно. Следовательно, должна поддерживаться возможность создания соответствующего количества файлов, содержащих данные нашей БД - по одному на каждый `close`, а так же возможность поиска значений по всем этим файлам.
* После использования метода `close`, `Entry` с уже записанным до этого в файл ключом может быть добавлено в последующие DAO еще несколько раз, база данных должна выдавать самое свежее для запрошенного ключа значение при поиске, игнорируя устаревшие, находящиеся в более ранних файлах.
* На данный момент метод `flush` будет вызываться только из метода `close`. Таким образом `flush` будет гарантированно выполняться однопоточно, также на данном этапе можно рассчитывать на то, что `get` и `upsert` в процессе выполнения метода `flush` вызываться не будут, однако во все остальное время `get` и `upsert` могут работать в многопоточном режиме - старые тесты на `concurrency` никуда не пропадают, но могут появиться и новые. Считаем, что поведение `get` и `upsert` после отработки метода `close` - `undefined`
### Report
Когда всё будет готово, присылайте pull request в ветку `main` со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!