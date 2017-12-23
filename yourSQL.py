# Akshat Tandon 201503001 
# akshat.tandon@research.iiit.ac.in

import sys
import csv
from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens

relations = {}
attributes = {}

requestTables = []
requestColumns = []
requestConditions = []


'''
	Reads the files associated with the data stored on disk
'''
def read_database():
	f = open('metadata.txt', 'rU')
	lines = f.readlines()
	i = 0
	while i < len(lines):
		line = lines[i].strip()
		if line == '<begin_table>':
			i += 1
			name = lines[i].strip().lower()
			# print 'Beginning a new table', name
			relations[name] = []
			attributes[name] = []
			i += 1
			y = 0
			while lines[i].strip() != '<end_table>':
				attributeName = lines[i].strip().lower()
				# print 'Adding attribute', attributeName
				attributes[name].append(attributeName)
				y += 1
				i += 1
			# print 'Attributes added: ',attributes[name]

		i += 1
	f.close()
	for name in relations.keys():
		f = open(name+'.csv', 'rt')
		# print 'Opened:', name
		try:
			reader = csv.reader(f)
			for row in reader:
				relations[name].append(row) 
				# print 'Relation read', relations[name][len(relations[name])-1]
		finally:
			f.close()


'''
	Parses the command given at commandline
'''
def parse_command(command):
	# Parsing code by Copyright (c) 2003,2016, Paul McGuire
		# define SQL tokens
	selectStmt = Forward()
	SELECT = Keyword("select", caseless=True)
	FROM = Keyword("from", caseless=True)
	WHERE = Keyword("where", caseless=True)

	ident          = Word( alphas, alphanums + "_$()" ).setName("identifier")
	columnName     = ( delimitedList( ident, ".", combine=True ) ).setName("column name").addParseAction(upcaseTokens)
	columnNameList = Group( delimitedList( columnName ) )
	tableName      = ( delimitedList( ident, ".", combine=True ) ).setName("table name").addParseAction(upcaseTokens)
	tableNameList  = Group( delimitedList( tableName ) )

	whereExpression = Forward()
	and_ = Keyword("and", caseless=True)
	or_ = Keyword("or", caseless=True)
	in_ = Keyword("in", caseless=True)

	E = CaselessLiteral("E")
	binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
	arithSign = Word("+-",exact=1)
	realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
	                                                         ( "." + Word(nums) ) ) + 
	            Optional( E + Optional(arithSign) + Word(nums) ) )
	intNum = Combine( Optional(arithSign) + Word( nums ) + 
	            Optional( E + Optional("+") + Word(nums) ) )

	columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
	whereCondition = Group(
	    ( columnName + binop + columnRval ) |
	    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
	    ( columnName + in_ + "(" + selectStmt + ")" ) |
	    ( "(" + whereExpression + ")" )
	    )
	whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

	# define the grammar
	selectStmt <<= (SELECT + ('*' | columnNameList)("columns") + 
	                FROM + tableNameList( "tables" ) + 
	                Optional(Group(WHERE + whereExpression), "")("where"))

	simpleSQL = selectStmt

	# define Oracle comment format, and ignore them
	oracleSqlComment = "--" + restOfLine
	simpleSQL.ignore( oracleSqlComment )

	parsedCommand = simpleSQL.parseString(command)

	lowerCols = [x.lower() for x in parsedCommand["columns"]]
	lowerTables = [x.lower() for x in parsedCommand["tables"]]
	if len(parsedCommand["where"]) == 1:
		lowerConditions = parsedCommand["where"]
	else:
		lowerConditions = [x.lower() for x in parsedCommand["where"][1]]
	return (lowerCols, lowerTables, lowerConditions)


def col2full(attributeName, tableNames):
	if attributeName == '*':
		return '*'

	aggregate = None
	aggregateFunctions = ['max', 'min', 'sum', 'avg', 'distinct']

	for aggName in aggregateFunctions:
		k = attributeName.find(aggName + '(')
		if k != -1:
			kq = attributeName[k:].find(')')
			if kq != -1:
				attributeName = attributeName[k+len(aggName)+1:kq]
				aggregate = aggName	
				# print 'Q: Attribute name', attributeName
				# print 'Q: Aggregate', aggregate

	fullName = None
	count = 0
	pos = attributeName.find('.')
	if pos != -1:
		tabName = attributeName.split('.')[0]
		attrName = attributeName.split('.')[1]
		# print 'Tabname:', tabName
		# print 'Attr Name:', attrName
		if attrName in attributes[tabName]:
			return attributeName
		else:
			print 'ERROR: Given attribute:'+ attrName +' does not exist in table:'+ tabName
			return None
	for relation in tableNames:
		# print attributes[relation]
		# print '### Checking in table:', relation
		# print '#Attribute', attributeName
		if attributeName in attributes[relation]:
			count += 1
			fullName = relation + '.' + attributeName
	if count == 0:
		print 'Error: Attribute not found:', attributeName
		return None
	if count > 1:
		print 'Error: Specify relation name with attribute, there are 2 or more relations with the same attribute: ',attributeName
		return None
	if count == 1:
		if aggregate != None:
			fullName = aggregate + '~' + fullName
		return fullName

def createBufferTable(requestTables):
	bufferTable = []
	bufferAttributes = {}
	bufferHeader = []
	count = 0
	if len(requestTables) == 0:
		print 'No relations mentioned'
	# copying first table
	# print 'Requested Tables:', requestTables
	for attrName in attributes[requestTables[0]]:
		# print '## passing', attrName
		attrName = requestTables[0] + '.' + attrName
		fullName = col2full(attrName, requestTables)
		# print 'count ', count
		bufferHeader.append(fullName)
		bufferAttributes[fullName] = count
		count += 1

	for row in relations[requestTables[0]]:
		bufferTable.append(row)
		# print 'Row:', row

	# if len(requestTables) == 1:
		# print 'Returning since only 1 table'
		# print 'Buffer table', bufferTable
		# print 'Buffer header', bufferHeader
		# print 'Buffer attributes', bufferAttributes
		# return
	if len(requestTables) != 1:
		for relationName in requestTables[1:]:
			tempBuffer = []
			for attrName in attributes[relationName]:
				attrName = relationName + '.' + attrName
				fullName = col2full(attrName, requestTables)
				bufferHeader.append(fullName)
				bufferAttributes[fullName] = count
				count += 1
			for bufRow in bufferTable:
				for row in relations[relationName]:
					currentRow = bufRow[:]
					currentRow += row
					tempBuffer.append(currentRow)
			bufferTable = tempBuffer

	# print 'Buffer table:: length', len(bufferTable) 
	# i=1
	# print 'Keys',bufferAttributes.keys()
	# print 'Values', bufferAttributes.values()
	return (bufferTable, bufferAttributes, bufferHeader)
	# for row in bufferTable:
	# 	print i, row
	# 	i += 1
	# print 'Buffer table:: length', len(bufferTable) 


def checkTables(tableNames):
	for name in tableNames:
		if name not in relations:
			print 'ERROR: table->', name , ' does not exist in the database'
			return False
	return True

def isNumeric(input):
	if input[0] == '-':
		input = input[1:]
	return all(char.isdigit() for char in input)

def suspectJoin(condition):
	if len(condition) == 3:
		second = condition[2].lower()
		if isNumeric(second) == False:
			return True
	return False

def checkCondition(condition, tableNames, row, bufferAttributes):
	if len(condition) != 3:
		print 'ERROR: Incorrect format for specifying the condition'
		sys.exit(0)
	# print 'Processing the condition', condition
	first = condition[0].lower()
	operator = condition[1].lower()
	second = condition[2].lower()
	# print 'First', first
	# print 'Operator', operator
	# print 'Second', second
	first = col2full(first, tableNames)
	if first == None:
		print 'ERROR: First is none, exiting'
		sys.exit(0)
	if isNumeric(second):
		if operator == '=':
			if int(row[bufferAttributes[first]]) == int(second):
				# print 'Matched'
				return True
			else:
				# print 'Not Matched'
				return False
		else:
			'ERROR: This operator not supported'
			sys.exit(0)
	else:
		# print 'Entered Join scenario'
		second = col2full(second, tableNames)
		# print 'Full second', second
		if second == None:
			print 'ERROR: Second is none, exiting'
			sys.exit(0)
		else:
			if operator == '=':
				if int(row[bufferAttributes[first]]) == int(row[bufferAttributes[second]]):
					# print 'Join matched'
					return True
				else:
					# print 'Join Not Matched'
					return False
			else:
				print 'ERROR: This operator not supported'
				sys.exit(0)


def calculateMax(index, bufferTable):
	flag = True
	maxv = 0
	for row in bufferTable:
		if flag:
			maxv = int(row[index])
			flag = False
		elif int(row[index]) > int(maxv):
			maxv = row[index]
	return maxv

def calculateMin(index, bufferTable):
	flag = True
	minv = 0
	for row in bufferTable:
		if flag:
			minv = int(row[index])
			flag = False
		elif int(row[index]) < int(minv):
			minv = row[index]
	return minv

def calculateSum(index, bufferTable):
	sumv = 0
	for row in bufferTable:
		sumv += int(row[index])
	return sumv

def calculateAvg(index, bufferTable):
	sumv = calculateSum(index, bufferTable) * 1.0
	avg = sumv/len(bufferTable)
	return avg



def process(columns, tableNames, conditions, bufferTable, bufferAttributes, bufferHeader):
	# print 'Conditions recieved:', conditions

	currentCols = []
	outputDict = {}
	aggregate = None
	numAggregates = 0
	numDistinct = 0
	currentAggregates = []
	if columns[0] != '*':
		masterColumns = columns
	else:
		masterColumns = bufferHeader
	for col in masterColumns:
			k = col.find('~')
			if k != -1:
				colSplit = col.split('~')
				aggregate = colSplit[0]
				if aggregate == 'distinct':
					numDistinct += 1
				else:
					numAggregates += 1
				currentAggregates.append(aggregate)
				# print 'P:Aggregate is:', aggregate
				col = colSplit[1]
				# print 'P:Column is:', col
			# print col + ',',
			currentCols.append(col)
	print 

	## handling aggregates
	if numAggregates > 0:
		if numAggregates != len(currentCols):
			print 'ERROR: Aggregates mixed with normal attributes, not allowed for now'
			sys.exit(0)
		else:
			for i in range(len(currentCols)):
				aggregate = currentAggregates[i]
				col = currentCols[i]
				if aggregate == 'max':
					temp = calculateMax(bufferAttributes[col], bufferTable)
					print str(temp) +',',
				elif aggregate == 'min':
					temp = calculateMin(bufferAttributes[col], bufferTable)
					print str(temp) +',',
				elif aggregate == 'sum':
					temp = calculateSum(bufferAttributes[col], bufferTable)
					print str(temp) +',',
				elif aggregate == 'avg':
					temp = calculateAvg(bufferAttributes[col], bufferTable)
					print str(temp) +',',
		print
		sys.exit(0)

	## handling distinct
	distinctFlag = False
	if numDistinct > 0:
		if numDistinct != len(currentCols):
			print 'ERROR: distinct should be applied to all the attributes'
			sys.exit(0)
		else:
			distinctFlag = True

	
	idx = 0
	dontPrint = None
	firstTime = True
	for row in bufferTable:
		flag = True
		# print 'Analyzing row', row
		
		if conditions != None:
			if len(conditions)==4:
				flag1 = checkCondition(conditions[1], tableNames, row, bufferAttributes)
				flag2 = checkCondition(conditions[3], tableNames, row, bufferAttributes)

				if suspectJoin(conditions[1]):
					dontPrint = conditions[1][2].lower()
				elif suspectJoin(conditions[3]):
					dontPrint = conditions[3][2].lower()
				if conditions[2] == 'and':
					flag = flag1 and flag2
				elif conditions[2] == 'or':
					flag = flag1 or flag2
				else:
					print 'ERROR:Operator not identified: ', condition[2]
					sys.exit(0)
			elif len(conditions) == 2:
				flag = checkCondition(conditions[1], tableNames, row, bufferAttributes)
				if suspectJoin(conditions[1]):
					dontPrint = conditions[1][2].lower()
			else:
				print 'ERROR: Where conditions exceeded/undefined'

		if firstTime:
			for col in currentCols:
				if dontPrint != None:
					if col != dontPrint:
						print col + ' , ',
				else:
					print col + ' , ',
			print 
			print 
			firstTime = False


		if flag:
			outputRow = ''
			for col in currentCols:
				# print '*'*10,
				if dontPrint != None:
					if col != dontPrint:
						outputRow += str(bufferTable[idx][bufferAttributes[col]]) +' , '
				else:
					outputRow += str(bufferTable[idx][bufferAttributes[col]]) +' , '
			if distinctFlag:
				if outputRow not in outputDict:
					outputDict[outputRow] = 1
					print outputRow
			else:
				print outputRow
			
		idx += 1



def main():
	if len(sys.argv) != 2:
		print 'Correct Usage: yourSQL.py "select * from ..." '
		
	read_database()
	requestColumns, requestTables, requestConditions = parse_command(sys.argv[1])

	if checkTables(requestTables) == False:
		sys.exit(0)

	# print 'Columns', requestColumns
	# print 'Where', requestConditions

	qualifiedColumns = []

	for name in requestColumns:
		# print 'Processing name', name
		fullName = col2full(name, requestTables)
		if fullName == None:
			sys.exit(0)
		if fullName != None:
			qualifiedColumns.append(fullName)
			# print 'Name made:', fullName
		# else:
		# 	print 'Attribute:', name, ' Not found'
	# print 'Tables', requestTables
	bufferTable, bufferAttributes, bufferHeader = createBufferTable(requestTables)
	# print 'length', len(requestConditions[0])
	lenConditions = len(requestConditions[0])
	if lenConditions == 0:
		process(qualifiedColumns, requestTables, None, bufferTable, bufferAttributes, bufferHeader)
		# process(qualifiedColumns, None, bufferTable, bufferAttributes)
	else:
		if lenConditions == 2 or lenConditions == 4:
			process(qualifiedColumns, requestTables,requestConditions[0], bufferTable, bufferAttributes, bufferHeader)
		else:
			print 'ERROR: Maximum number of conditions exceeded'
			sys.exit(0)

	


if __name__=='__main__':
	main()