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
Следует иметь ввиду, что в дальнейшем в этом файле будет происходить бинарный поиск без полной загрузки в память (но на данном этапе, это можно игнорировать).

### Report
Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!
