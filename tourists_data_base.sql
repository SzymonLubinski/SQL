--Tworzenie tabeli, która będzie zawierać informacje o turystach spedzających noce w wybranych Państwach.
create table UE_COUNTRIES
(
    id_country 		number primary key,
    TIME 			varchar2(7) not null,
    Belgium 		number,
    Bulgaria 		number,
    Czechia 		number,
    Denmark 		number,
    Germany 		number,
    Estonia 		number,
    Ireland 		number,
    Greece 			number,
    Spain 			number,
    France 			number,
    Croatia 		number,
    Italy 			number,
    Cyprus 			number,
    Latvia 			number,
    Lithuania 		number,
    Luxembourg 		number,
    Hungary 		number,
    Malta 			number,
    Netherlands 	number,
    Austria 		number,
    Poland 			number,
    Portugal 		number,
    Romania 		number,
    Slovenia 		number,
    Slovakia 		number,
    Finland 		number,
    Sweden 			number,
    Iceland 		number,
    Liechtenstein   number,
    Norway 			number,
    Switzerland 	number,
    United_Kingdom  number,
    Montenegro 		number,
    North_Macedonia number,
    Albania 		number,
    Serbia 			number,
    Turkey 			number
);

--Wprowadzenie ograniczeń do tabeli UE_COUNTRIES.
alter table UE_COUNTRIES
add check
(
    Belgium 		< 8000000000 and
    Bulgaria 		< 8000000000 and
    Czechia 		< 8000000000 and
    Denmark 		< 8000000000 and
    Germany 		< 8000000000 and
    Estonia 		< 8000000000 and
    Ireland 		< 8000000000 and
    Greece 			< 8000000000 and
    Spain 			< 8000000000 and
    France 			< 8000000000 and
    Croatia 		< 8000000000 and
    Italy 			< 8000000000 and
    Cyprus 			< 8000000000 and
    Latvia 			< 8000000000 and
    Lithuania 		< 8000000000 and
    Luxembourg 		< 8000000000 and
    Hungary 		< 8000000000 and
    Malta 			< 8000000000 and
    Netherlands 	< 8000000000 and
    Austria 		< 8000000000 and
    Poland 			< 8000000000 and
    Portugal 		< 8000000000 and
    Romania 		< 8000000000 and
    Slovenia 		< 8000000000 and
    Slovakia 		< 8000000000 and
    Finland 		< 8000000000 and
    Sweden 			< 8000000000 and
    Iceland 		< 8000000000 and
    Liechtenstein   < 8000000000 and
    Norway 			< 8000000000 and
    Switzerland 	< 8000000000 and
    United_Kingdom  < 8000000000 and
    Montenegro 		< 8000000000 and
    North_Macedonia < 8000000000 and
    Albania 		< 8000000000 and
    Serbia 			< 8000000000 and
    Turkey 			< 8000000000
);

--Tworzenie tabeli, która będzie zawierać informacje o aktualizacjach w tabeli UE_COUNTRIES.
create table update_data
(
	id_time 	number primary key,
	update_date date,
	new_data 	varchar2(2000)
);

--Tworzenie procedury, która będzie przekazywać do tabeli update_data informacje o aktualizacjach w tabeli UE_COUNTRIES.

CREATE OR REPLACE PROCEDURE PUD
IS
sum_of_the_data number;

CURSOR c1 IS
select 
	count(Belgium 			) +
	count(Bulgaria 			) +
	count(Czechia 			) +
	count(Denmark 			) +
	count(Germany 			) +
	count(Estonia 			) +
	count(Ireland 			) +
	count(Greece 			) +
	count(Spain 			) +
	count(France 			) +
	count(Croatia 			) +
	count(Italy 			) +
	count(Cyprus 			) +
	count(Latvia 			) +
	count(Lithuania 		) +
	count(Luxembourg 		) +
	count(Hungary 			) +
	count(Malta 			) +
	count(Netherlands 		) +
	count(Austria 			) +
	count(Poland 			) +
	count(Portugal 			) +
	count(Romania 			) +
	count(Slovenia 			) +
	count(Slovakia 			) +
	count(Finland 			) +
	count(Sweden 			) +
	count(Iceland 			) +
	count(Liechtenstein   	) +
	count(Norway 			) +
	count(Switzerland 		) +
	count(United_Kingdom  	) +
	count(Montenegro 		) +
	count(North_Macedonia 	) +
	count(Albania 			) +
	count(Serbia 			) +
	count(Turkey 			)
from ue_countries;

BEGIN
	
OPEN c1;
FETCH c1 INTO sum_of_the_data;

INSERT INTO update_data
(
	id_time 	,	
	update_date ,
	new_data 	
)
VALUES
(
	seq_update_data.nextval	,
	SYSDATE					,
	sum_of_the_data
);
CLOSE c1;
END;
/

--Tworzenie sekwencji, która będzie nadawać kolejne id_time do tabeli update_data.
CREATE SEQUENCE seq_update_data
MINVALUE 1
START WITH 1
INCREMENT BY 1
CACHE 100

--Tworzenie kalendarza według, którego będzie się odbywać aktualizacja.

BEGIN 
	DBMS_SCHEDULER.create_schedule 
		(
		schedule_name   => 'schedule_1',
		start_date      => SYSTIMESTAMP,
		repeat_interval => 'freq=minutely; bysecond=0;',
		end_date        => NULL,
		comments		=> 'Update every minute'
		);
END;
/

BEGIN
	DBMS_SCHEDULER.drop_schedule (schedule_name => 'schedule_1');
END;
/

--Tworzenie programu uruchamiającego procedurę PUD.
BEGIN
DBMS_SCHEDULER.create_program
		(
		program_name   => 'program_1',
		program_type   => 'STORED_PROCEDURE',
		program_action => 'PUD',
		enabled        => TRUE,
		comments	   => 'Start procedure PUD'
		);
END;
/

--Tworzenie zadania, które będzie wywoływać program_1 według harmonogramu schedule_1.

BEGIN 
	DBMS_SCHEDULER.create_job
		(
		job_name   		=> 'job_1',
		program_name   	=> 'program_1',
		schedule_name   => 'schedule_1',
		enabled        	=> TRUE,
		comments	   	=> 'start verification'
		);
END;
/

--SQL LOADER
--Do użycia programu SQL LOADER potrzbny będzie kontrony plik.ctl o zawartości:
--LOAD DATA
--INFILE 'lokalizacja pliku z danymi\plik.csv'
--TRUNCATE
--INTO TABLE UE_COUNTRIES
--fields terminated by ","
--(
--	id_country 		,
--    TIME 			,
--    Belgium 		,
--    Bulgaria 		,
--    Czechia 		,
--    Denmark 		,
--    Germany 		,
--    Estonia 		,
--    Ireland 		,
--    Greece 			,
--    Spain 			,
--    France 			,
--    Croatia 		,
--    Italy 			,
--    Cyprus 			,
--    Latvia 			,
--    Lithuania 		,
--    Luxembourg 		,
--    Hungary 		,
--    Malta 			,
--    Netherlands 	,
--    Austria 		,
--    Poland 			,
--    Portugal 		,
--    Romania 		,
--    Slovenia 		,
--    Slovakia 		,
--    Finland 		,
--    Sweden 			,
--    Iceland 		,
--    Liechtenstein   ,
--    Norway 			,
--    Switzerland 	,
--    United_Kingdom  ,
--    Montenegro 		,
--    North_Macedonia ,
--    Albania 		,
--    Serbia 			,
--    Turkey 			
--)

--Polecenie importujące dane w wierszu poleceń.
--Uruchomienie wiersza poleceń z lokalizacji pliku kontrolnego.
--sqlldr userid=user_name/password control='lokalizacja pliku kontrolnego.ctl\plik.ctl'