import mysql.connector


#Start the mysql connection to the local database
#Home
#cnx = mysql.connector.connect(user='root', password='root', database='noi_donations')

#Work
cnx = mysql.connector.connect(user='root', database='donations')
#Calls the cursor method to move through database results
cursor = cnx.cursor()

#Queries

#This gets contact info for all contacts in Salesforce
all_info_query = ("""
	SELECT Contact_Contact_ID, Contact_First_Name, Contact_Last_Name,
		Contact_Email, Contact_Household_Phone, Contact_Work_Phone
		FROM donors
		WHERE char_length(Contact_Contact_ID)>1
		GROUP BY HEX(Contact_Contact_ID)
		
""")
#This gets contact info, max, sum, and count of donations
donations_query = ("""
	SELECT Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, 
	Contact_Email, Contact_Household_Phone, Contact_Work_Phone,
	MAX(Donation_Amount) as 'Maximum Donation', 
	SUM(Donation_Amount) as 'Sum of Donations', 
	count(Donation_Amount) as 'Count of Donations',
	MAX(Donation_Date) as 'Last Donation Date'
	FROM donors
	WHERE Donor_Type LIKE '%Individual%'
	GROUP BY HEX(Contact_Contact_ID)
	ORDER BY SUM(Donation_Amount) DESC
	
""")
#This gets contact info, max, sum, and count of grants
grants_query =("""
	SELECT Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, 
	Contact_Email, Contact_Household_Phone, Contact_Work_Phone,
	MAX(Donation_Amount) as 'Maximum Grant', 
	SUM(Donation_Amount) as 'Sum of Grants', 
	count(Donation_Amount) as 'Count of Grants',
	MAX(Donation_Date) as 'Last Grant Date'
	FROM donors
	WHERE Donor_Type LIKE '%Grant%'
	GROUP BY HEX(Contact_Contact_ID)
	ORDER BY SUM(Donation_Amount) DESC
""")
#This gets contact info, max, sum, and count of training fees
training_fees_query =("""
	SELECT Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, 
	Contact_Email, Contact_Household_Phone, Contact_Work_Phone,
	MAX(Donation_Amount) as 'Maximum Fee', 
	SUM(Donation_Amount) as 'Sum of Fees', 
	count(Donation_Amount) as 'Count of Fees',
	MAX(Donation_Date) as 'Last Fee Date'
	FROM donors
	WHERE Donor_Type LIKE 'Training Fee'
	GROUP BY HEX(Contact_Contact_ID)
	ORDER BY SUM(Donation_Amount) DESC
""")
#This gets contact info, max, sum, and count of ticket purchases
tickets_query =("""
	SELECT Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, 
	Contact_Email, Contact_Household_Phone, Contact_Work_Phone,
	MAX(Donation_Amount) as 'Maximum Ticket', 
	SUM(Donation_Amount) as 'Sum of Tickets', 
	count(Donation_Amount) as 'Count of Tickets',
	MAX(Donation_Date) as 'Last Ticket Date'
	FROM donors
	WHERE Donor_Type LIKE 'Fee%'
	GROUP BY HEX(Contact_Contact_ID)
	ORDER BY SUM(Donation_Amount) DESC
""")
#This gets contact info, max, sum, and count of all payments
all_payments_query =("""
	SELECT Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, 
	Contact_Email, Contact_Household_Phone, Contact_Work_Phone,
	MAX(Donation_Amount) as 'Maximum of All', 
	SUM(Donation_Amount) as 'Sum of All', 
	count(Donation_Amount) as 'Count of All',
	MAX(Donation_Date) as 'Last Date of All'
	FROM donors
	WHERE Donor_Type LIKE 'Fee%'
	OR Donor_Type LIKE 'Training Fee'
	OR Donor_Type LIKE '%Grant%'
	OR Donor_Type LIKE '%Individual%'
	GROUP BY HEX(Contact_Contact_ID)
	ORDER BY SUM(Donation_Amount) DESC
""")

#Start the call to the database to get all_info
cursor.execute(all_info_query)
all_info = {}

#Create a list of dictionaries from the cursor running all_info
for (Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone) in cursor:
	single_info = [Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone]
	for item in single_info:
	#Handle the None type error by setting item to blank
		if item is None:
			item = ''
	all_info[Contact_Contact_ID] = single_info
cursor.close()

cursor = cnx.cursor()
#Create a dictionary for the donation_info query.
cursor.execute(donations_query)
donation_info = {}
for (Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Donation, Sum_of_Donations, Count_of_Donations, Last_Donation_Date) in cursor:
	single_info = [Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Donation, Sum_of_Donations, Count_of_Donations, Last_Donation_Date]
	for item in single_info:
		if item is None:
			item = ''
	donation_info[Contact_Contact_ID] = single_info
cursor.close()

cursor = cnx.cursor()
#Create a dictionary for the grants query.
cursor.execute(grants_query)
grants_info = {}
for (Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Grant, Sum_of_Grants, Count_of_Grants, Last_Grant_Date) in cursor:
	single_info = [Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Grant, Sum_of_Grants, Count_of_Grants, Last_Grant_Date]
	for item in single_info:
		if item is None:
			item = ''
	grants_info[Contact_Contact_ID] = single_info
cursor.close()

cursor = cnx.cursor()
#Create a dictionary for the training fees query.
cursor.execute(training_fees_query)
training_fees_info = {}
for (Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Training_Fee, Sum_of_Training_Fees, Count_of_Training_Fees, Last_Training_Fee_Date) in cursor:
	single_info = [Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Training_Fee, Sum_of_Training_Fees, Count_of_Training_Fees, Last_Training_Fee_Date]
	for item in single_info:
		if item is None:
			item = ''
	training_fees_info[Contact_Contact_ID] = single_info
cursor.close()

cursor = cnx.cursor()
#Create a dictionary for the tickets query.
cursor.execute(tickets_query)
tickets_info = {}
for (Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Ticket, Sum_of_Tickets, Count_of_Tickets, Last_Ticket_Date) in cursor:
	single_info = [Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_Ticket, Sum_of_Tickets, Count_of_Tickets, Last_Ticket_Date]
	for item in single_info:
		if item is None:
			item = ''
	tickets_info[Contact_Contact_ID] = single_info
cursor.close()

cursor = cnx.cursor()
#Create a dictionary for the all payments query.
cursor.execute(all_payments_query)
all_payments_info = {}
for (Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_of_All, Sum_of_All, Count_of_All, Last_Date_of_All) in cursor:
	single_info = [Contact_Contact_ID, Contact_First_Name, Contact_Last_Name, Contact_Email, Contact_Household_Phone, Contact_Work_Phone,Maximum_of_All, Sum_of_All, Count_of_All, Last_Date_of_All]
	for item in single_info:
		if item is None:
			item = ''
	all_payments_info[Contact_Contact_ID] = single_info
cursor.close()

#Close connection to localhost
cnx.close()

#Open connection to ActionKit
cnx = mysql.connector.connect(host='client-db.actionkit.com', user='noi_anupama', password='92semKqWFdDM',database='ak_noi')
cursor = cnx.cursor()

#AlumNOI Query
ak_query=("""
	SELECT core_user.email
	FROM core_user
	JOIN core_action
	ON (core_user.id = core_action.user_id)
	JOIN core_page
	ON (core_action.page_id=core_page.id)
	WHERE core_user.subscription_status = 'subscribed'
	AND (core_page.title LIKE 'NMT%' COLLATE utf8_bin
	OR core_page.title LIKE 'DBC%' COLLATE utf8_bin
	OR core_page.title LIKE 'DT%' COLLATE utf8_bin
	OR core_page.title LIKE 'MBT%' COLLATE utf8_bin
	OR core_page.title LIKE 'NMBC%' COLLATE utf8_bin
	OR core_page.title LIKE 'NMT%' COLLATE utf8_bin
	OR core_page.title LIKE 'NOD%' COLLATE utf8_bin
	OR core_page.title LIKE 'NOF%' COLLATE utf8_bin
	OR core_page.title LIKE 'NOISE%' COLLATE utf8_bin
	OR core_page.title LIKE 'RC%' COLLATE utf8_bin
	OR core_page.title LIKE 'SRC%' COLLATE utf8_bin)	
""")

cursor.execute(ak_query)
ak_info = []
for (email) in cursor:
	ak_info.append(str(email).lower())
cursor.close()
cnx.close()

#Add donation information	
for contact_id, info_list in all_info.iteritems():
	if contact_id in donation_info.keys():
		info_list.extend(donation_info.get(contact_id)[6:])
	else:
		info_list.extend(['', '', '', ''])
		
#Add grant information		
for contact_id, info_list in all_info.iteritems():
	if contact_id in grants_info.keys():
		info_list.extend(grants_info.get(contact_id)[6:])
	else:
		info_list.extend(['', '', '', ''])	
		
#Add training fee information		
for contact_id, info_list in all_info.iteritems():
	if contact_id in training_fees_info.keys():
		info_list.extend(training_fees_info.get(contact_id)[6:])
	else:
		info_list.extend(['', '', '', ''])		
		
#Add ticket information		
for contact_id, info_list in all_info.iteritems():
	if contact_id in tickets_info.keys():
		info_list.extend(tickets_info.get(contact_id)[6:])
	else:
		info_list.extend(['', '', '', ''])		

#Add all payments information		
for contact_id, info_list in all_info.iteritems():
	if contact_id in all_payments_info.keys():
		info_list.extend(all_payments_info.get(contact_id)[6:])
	else:
		info_list.extend(['', '', '', ''])

#Add alumNOI info
for info_list in all_info.itervalues():
	print 'Email is {0}'.format(info_list[3])
	if info_list[3].lower() in ak_info:
		info_list.append('1')
	else:
		info_list.append('')
		
#print all_info

def dict_of_lists_to_csv (dict, headers):
	csv_string = ""
	for header in headers:
		csv_string +=(header+',')
	csv_string = csv_string[:-1]
	csv_string +='\n'
	#print csv_string
	for v in dict.itervalues():
		#print v
		for e in v:
			if e is None:
				e=''
			try:
				csv_string += str(e)+','
			except UnicodeEncodeError:
				e='Unicode Error'
				csv_string += e+','
			#csv_string=csv_string.encode('ascii','ignore')
			
			#print csv_string
		csv_string +=('\n')
	return csv_string
	
#all the headers:	
headers = ['Contact_Contact_ID', 
	'Contact_First_Name', 
	'Contact_Last_Name', 
	'Contact_Email', 
	'Contact_Household_Phone', 
	'Contact_Work_Phone', 
	'Maximum_Donation', 
	'Sum_of_Donations', 
	'Count_of_Donations', 
	'Last_Donation_Date', 
	'Maximum_Grant', 
	'Sum_of_Grants', 
	'Count_of_Grants', 
	'Last_Grant_Date', 
	'Maximum_Training_Fee', 
	'Sum_of_Training_Fees', 
	'Count_of_Training_Fees', 
	'Last_Training_Fee_Date', 
	'Maximum_Ticket', 
	'Sum_of_Tickets', 
	'Count_of_Tickets', 
	'Last_Ticket_Date', 
	'Maximum_of_All', 
	'Sum_of_All', 
	'Count_of_All', 
	'Last_Date_of_All',
	'AlumNOI']

#Write the data to the output file
with open ("all_sum_count_max_alumNOI.csv", "w") as append_emp:
	append_emp.write(dict_of_lists_to_csv(all_info, headers))
