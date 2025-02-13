-- Ejercicio 2

create table bootcamps (
    bootcamp_id serial primary key ,  
    name  varchar(20) NOT NULL,  
    start_date  date NOT NULL,
    end_date date NOT NULL
);

create table teachers (
    teacher_id serial primary key,
    name varchar(50) NOT NULL,
    surname varchar(50) NOT NULL,
    email varchar(100) NOT NULL,
    birthday date NOT NULL
);

create table teachers_bootcamp (
    teachers_bootcamp_id serial primary key,
    teacher_id int NOT NULL,
    bootcamp_id int NOT NULL,
    foreign key  (teacher_id) references teachers(teacher_id),
    foreign key  (bootcamp_id) references bootcamps(bootcamp_id)
);


create table students (
    student_id serial primary key,
    name varchar(50) NOT NULL,
    surname varchar(50) NOT NULL,
    birthday date NOT NULL
);

create table registro (
    registro_id serial primary key NOT NULL,
    student_id int NOT NULL,
    bootcamp_id int NOT NULL,
    foreign key (student_id) references students(student_id),
    foreign key  (bootcamp_id) references bootcamps(bootcamp_id)
);

create table subjects (
    subject_id serial primary key,
    name varchar(50) NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL
);
    
create table troncal_subjects (
	troncal_subjects_id serial primary key,
    bootcamp_id int NOT NULL,
    subject_id int NOT NULL,
    foreign key (subject_id) references subjects(subject_id),
    foreign key (bootcamp_id) references bootcamps(bootcamp_id)
);