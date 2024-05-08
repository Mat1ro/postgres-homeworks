-- 1. Создать таблицу student с полями student_id serial, first_name varchar, last_name varchar, birthday date, phone varchar
CREATE TABLE student
(
    student_id serial,
    first_name varchar,
    birthday   date,
    phone      varchar
);

-- 2. Добавить в таблицу student колонку middle_name varchar
ALTER TABLE student
    ADD COLUMN middle_name varchar;

-- 3. Удалить колонку middle_name
ALTER TABLE student
    DROP COLUMN middle_name;

-- 4. Переименовать колонку birthday в birth_date
ALTER TABLE student
    RENAME birthday TO birth_date;

-- 5. Изменить тип данных колонки phone на varchar(32)
ALTER TABLE student
    ALTER COLUMN phone SET DATA TYPE varchar(32);

-- 6. Вставить три любых записи с авто генерацией идентификатора
INSERT INTO student (first_name, birth_date, phone)
VALUES ('Roman', '2006-05-30', '+79999999999'),
       ('Pavel', '2006-06-20', '+70000000000'),
       ('Denis', '2006-06-13', '+77777777777');

-- 7. Удалить все данные из таблицы со сбросом идентификатор в исходное состояние
TRUNCATE TABLE student RESTART IDENTITY;