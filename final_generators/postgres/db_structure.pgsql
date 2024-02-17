CREATE table courses(
    id serial PRIMARY KEY,
    title varchar(300) not null unique,
    duration integer,
    description_id varchar(50)
);

CREATE table specialities(
    id serial PRIMARY KEY,
    title varchar(200) not null,
    code varchar(50) not null unique,
    study_duration int
);

CREATE table departments(
    id serial PRIMARY KEY,
    title varchar(200) not null unique,
    institute_id int not null,
    main_speciality_id int not null,
    foreign key (main_speciality_id) references specialities(id)
);

create table groups (
    id serial PRIMARY KEY,
    number varchar(50) not null UNIQUE,
    department_id int not null,
    speciality_id int not null,
    foreign key (department_id) REFERENCES departments(id),
    foreign key (speciality_id) references specialities(id)
);

create table lecture_types (
    id serial PRIMARY KEY,
    type varchar(50) not null unique
);

create table lectures(
    id serial PRIMARY KEY,
    annotation varchar(200) not null,
    type_id int not null,
    course_id int not null,
    requirements varchar(350) not null,
    foreign key (type_id) REFERENCES lecture_types(id),
    foreign KEY (course_id) REFERENCES courses(id)
);

create table lecture_materials (
    id serial PRIMARY KEY,
    lecture_id int not null,
    materials_id varchar(200),
    foreign key(lecture_id) REFERENCES lectures(id)
);

create table students (
    id serial PRIMARY KEY,
    name varchar(200),
    group_id int not null,
    passbook_number varchar(50) not null UNIQUE,
    foreign key (group_id) REFERENCES groups (id)
);

create table timetable (
    id serial PRIMARY KEY,
    group_id int not null,
    lecture_id int not null,
    date timestamp not null,
    foreign key(group_id) references groups(id),
    foreign key(lecture_id) references lectures(id)
);

create table visits (
    student_id int not null,
    lecture_id int not null,
    date timestamp not null,
    foreign key(student_id) REFERENCES students(id),
    foreign key(lecture_id) references lectures(id)
) partition by range(date);

create table visits_202305 partition of visits 
    for values from ('2023-05-01') to ('2023-05-31');

create table visits_202306 partition of visits 
    for values from ('2023-06-01') to ('2023-06-30');

create table visits_202307 partition of visits 
    for values from ('2023-07-01') to ('2023-07-31');