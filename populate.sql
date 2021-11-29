INSERT INTO Airlines (AIRLINE_ID, AIRLINE_NAME) VALUES 
	('UA', 'United Air Lines Inc.'),
	('AA', 'American Airlines Inc.'),
	('US', 'US Airways Inc.'),
	('F9', 'Frontier Airlines Inc.'),
	('B6', 'JetBlue Airways'),
	('OO', 'Skywest Airlines Inc.'),
	('AS', 'Alaska Airlines Inc.'),
	('NK', 'Spirit Air Lines'),
	('WN', 'Southwest Airlines Co.'),
	('DL', 'Delta Air Lines Inc.'),
	('EV', 'Atlantic Southeast Airlines'),
	('HA', 'Hawaiian Airlines Inc.'),
	('MQ', 'American Eagle Airlines Inc.'),
	('VX', 'Virgin America');


INSERT INTO Airports (AIRPORT_ID, AIRPORT_NAME) VALUES 
	('ABE', 'Lehigh Valley International Airport'),
	('ABI', 'Abilene Regional Airport'),
	('ABQ', 'Albuquerque International Sunport'),
	('ABR', 'Aberdeen Regional Airport'),	
	('ABY', 'Southwest Georgia Regional Airport'),
	('ACK', 'Nantucket Memorial Airport'),
	('ACT', 'Waco Regional Airport'),
	('ACV', 'Arcata Airport'),
	('ACY', 'Atlantic City International Airport'),
	('ADK', 'Adak Airport');


INSERT INTO Flight_Airline (FLIGHT_ID, AIRLINE) VALUES 
	('AS98', 'AS'),
	('AA2336', 'AA'),
	('US840', 'US'),
	('AA258', 'AA'),
	('AS135', 'AS'),
	('DL806', 'DL'),
	('NK612', 'NK'),
	('US2013', 'US'),
	('AA1112', 'AA'),
	('DL1173', 'DL');


INSERT INTO Flight_Route (FLIGHT_ID, ORIGIN_AIRPORT, DESTINATION_AIRPORT) VALUES 
	('AS98', 'ABE', 'ABI'),
	('AA2336', 'ABI', 'ABQ'),
	('US840', 'ABQ', 'ABR'),
	('AA258', 'ABR', 'ABY'),
	('AS135', 'ABY', 'ACK'),
	('DL806', 'ACK', 'ACT'),
	('NK612', 'ACT', 'ACV'),
	('US2013', 'ACV', 'ACY'),
	('AA1112', 'ACY', 'ADK'),
	('DL1173', 'ADK', 'ABE');
