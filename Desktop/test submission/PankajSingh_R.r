#R Program that reads the Positions File and Transaction file to generate the 


#installing all the requisite libraries
install.packages(c("RMySQL","RJSONIO","sqldf", "dplyr"))
#importing all the installed libraries
library(RJSONIO)
library(sqldf)
library (RMySQL) #if the file needs to be inserted as a database table

#pre-requisite the working directin needs to be set and all libraries needs to be pre-installed

#reading the Input_StartOfDay_Positions.txt file as a table 

Input_StartOfDay_Positions  = read.table("Input_StartOfDay_Positions.txt", header = TRUE, sep= ",")

#Creating dataframe for the Input_StartOfDay_Positions file 
Input_StartOfDay_Positions = data.frame(Input_StartOfDay_Positions)


#reading the JSON Transaction file as a table

Input_Transactions <- fromJSON("Input_Transactions.json")

Input_Transactions <- lapply(Input_Transactions, function(x) {
  x[sapply(x, is.null)] <- NA
  unlist(x)
})

Input_Transactions = do.call("rbind", Input_Transactions)

#Converting the file into a dataframe
Input_Transactions = data.frame(Input_Transactions)

#creating the Expected_EndOfDay_Positions table as a variable 
Expected_EndOfDay_Positions = sqldf("select distinct b.Instrument,b.Account, b.AccountType, 
					   MAX(CASE WHEN (a.TransactionType = 'B' and b.AccountType = 'E') THEN (coalesce(b.Quantity,0) + coalesce(a.TransactionQuantity,0)) 
					        WHEN (a.TransactionType = 'B' and b.AccountType = 'I') THEN (coalesce(b.Quantity,0) - coalesce(a.TransactionQuantity,0)) 
							WHEN (TransactionType = 'S' and b.AccountType = 'E') THEN (coalesce(b.Quantity,0) - coalesce(a.TransactionQuantity,0))
					        WHEN (TransactionType = 'S' and b.AccountType = 'I') THEN (coalesce(b.Quantity,0) + coalesce(a.TransactionQuantity,0))
							ELSE Quantity
					    END) AS Quantity,
						(b.Quantity - CASE WHEN (a.TransactionType = 'B' and b.AccountType = 'E') THEN (coalesce(b.Quantity,0) + coalesce(a.TransactionQuantity,0)) 
					        WHEN (a.TransactionType = 'B' and b.AccountType = 'I') THEN (coalesce(b.Quantity,0) - coalesce(a.TransactionQuantity,0)) 
							WHEN (TransactionType = 'S' and b.AccountType = 'E') THEN (coalesce(b.Quantity,0) - coalesce(a.TransactionQuantity,0))
					        WHEN (TransactionType = 'S' and b.AccountType = 'I') THEN (coalesce(b.Quantity,0) + coalesce(a.TransactionQuantity,0))
							ELSE b.Quantity
					    END) As Delta
		FROM Input_StartOfDay_Positions b left join Input_Transactions a on b.Instrument = a.Instrument
		GROUP BY b.Instrument,b.Account, b.AccountType" , drv = "SQLite")
#Checking the create Data Frame 
head(Expected_EndOfDay_Positions)
									
#writing the output to text file in the working directory
write.table(Expected_EndOfDay_Positions, "Expected_EndOfDay_Positions_Create.txt", col.names= TRUE, sep = ",")

############### in case the files needed to be loaded into the database ###############
con = dbConnect (RMySQL::MySQL(), host = "localhost", port = 3306, dbname = "<input DBNAME>", username = "root", password = "<input your password>")
dbWriteTable(con,'Input_StartOfDay_Positions', Input_StartOfDay_Positions)
dbWriteTable(con,'Input_Transactions', Input_Transactions)
dbWriteTable(con,'Expected_EndOfDay_Positions', Expected_EndOfDay_Positions)








					
																					