#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;

#define sizeOfAttribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t COLUMN_EMAIL_SIZE = 255;

struct Row
{
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
};

const uint32_t ID_SIZE = sizeOfAttribute(Row, id);
const uint32_t USERNAME_SIZE = sizeOfAttribute(Row, username);
const uint32_t EMAIL_SIZE = sizeOfAttribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Pager
{
	int file_descriptor;
	uint32_t file_length;
	void *pages[TABLE_MAX_PAGES];
};

struct Table
{
	Pager *pager;
	uint32_t numRows;
};

enum ExecuteResult
{
	EXECUTE_TABLE_FULL,
	EXECUTE_SUCCESS
};

enum StatementType_t
{
	STATEMENT_INSERT,
	STATEMENT_SELECT
};

struct Statement
{
	StatementType_t type;
	Row row;
};

enum PrepareResult_t
{
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT,
	PREPARE_SYNTAX_ERROR,
	PREPARE_STRING_TOO_LONG,
	PREPARE_NEGATIVE_ID
};

enum MetaCommandResult_t
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
};

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{
	if (pager->pages[page_num] == NULL)
	{
		printf("Tried to flush null page\n");
		exit(EXIT_FAILURE);
	}

	off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

	if (offset == -1)
	{
		printf("Error seeking: %d\n", errno);
		exit(EXIT_FAILURE);
	}

	ssize_t bytes_written =
			write(pager->file_descriptor, pager->pages[page_num], size);

	if (bytes_written == -1)
	{
		printf("Error writing: %d\n", errno);
		exit(EXIT_FAILURE);
	}
}

void db_close(Table *table)
{
	Pager *pager = table->pager;
	uint32_t num_full_pages = table->numRows / ROWS_PER_PAGE;

	for (uint32_t i = 0; i < num_full_pages; i++)
	{
		if (pager->pages[i] == NULL)
		{
			continue;
		}
		pager_flush(pager, i, PAGE_SIZE);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	// There may be a partial page to write to the end of the file
	// This should not be needed after we switch to a B-tree
	uint32_t num_additional_rows = table->numRows % ROWS_PER_PAGE;
	if (num_additional_rows > 0)
	{
		uint32_t page_num = num_full_pages;
		if (pager->pages[page_num] != NULL)
		{
			pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
			free(pager->pages[page_num]);
			pager->pages[page_num] = NULL;
		}
	}

	int result = close(pager->file_descriptor);
	if (result == -1)
	{
		printf("Error closing db file.\n");
		exit(EXIT_FAILURE);
	}

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
	{
		void *page = pager->pages[i];
		if (page)
		{
			free(page);
			pager->pages[i] = NULL;
		}
	}

	free(pager);
}

void *get_page(Pager *pager, uint32_t page_num)
{
	if (page_num > TABLE_MAX_PAGES)
	{
		cout << "Tried to fetch page number out of bounds. " << page_num << " > " << TABLE_MAX_PAGES << endl;
		exit(EXIT_FAILURE);
	}

	if (pager->pages[page_num] == NULL)
	{
		// Cache miss. Allocate memory and load from file
		void *page = malloc(PAGE_SIZE);
		uint32_t num_pages = pager->file_length / PAGE_SIZE;

		// We might save a partial page at the end of the file
		if (pager->file_length % PAGE_SIZE)
		{
			num_pages += 1;
		}

		if (page_num <= num_pages)
		{
			lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
			if (bytes_read == -1)
			{
				printf("Error reading file: %d\n", errno);
				exit(EXIT_FAILURE);
			}
		}

		pager->pages[page_num] = page;
	}

	return pager->pages[page_num];
}

Pager *pager_open(const char *filename)
{
	int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

	if (fd == -1)
	{
		cout << "Unable to open file" << endl;
		exit(EXIT_FAILURE);
	}

	off_t file_length = lseek(fd, 0, SEEK_END);
	Pager *pager = new Pager();
	pager->file_descriptor = fd;
	pager->file_length = file_length;

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
	{
		pager->pages[i] = NULL;
	}

	return pager;
}

Table *db_open(const char *filename)
{
	Pager *pager = pager_open(filename);
	uint32_t numRows = pager->file_length / ROW_SIZE;
	Table *table = new Table();
	table->pager = pager;
	table->numRows = numRows;

	return table;
}

void *rowSlot(Table *table, uint32_t rowNum)
{
	uint32_t pageNum = rowNum / ROWS_PER_PAGE;
	void *page = get_page(table->pager, pageNum);
	uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
	uint32_t byteOffset = rowOffset * ROW_SIZE;

	return (char *)page + byteOffset;
}

void serializeRow(Row *source, void *destination)
{
	memcpy((char *)destination + ID_OFFSET, &(source->id), ID_SIZE);
	memcpy((char *)destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
	memcpy((char *)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserializeRow(void *source, Row *destination)
{
	memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination->username), (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(destination->email), (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

PrepareResult_t prepareInsert(string input, Statement *statement)
{
	statement->type = STATEMENT_INSERT;

	string id_string;
	string username;
	string email;

	stringstream ss;
	ss << input.substr(6);
	ss >> id_string >> username >> email;

	if (ss.fail() || id_string.empty() || username.empty() || email.empty())
	{
		return PREPARE_SYNTAX_ERROR;
	}

	int id = stoi(id_string);

	if (id < 0)
	{
		return PREPARE_NEGATIVE_ID;
	}

	if (username.length() > COLUMN_USERNAME_SIZE || email.length() > COLUMN_EMAIL_SIZE)
	{
		return PREPARE_STRING_TOO_LONG;
	}

	statement->row.id = id;
	strcpy(statement->row.username, username.c_str());
	strcpy(statement->row.email, email.c_str());

	return PREPARE_SUCCESS;
}

int prepareStatement(string input, Statement *statement)
{
	if (input.substr(0, 6) == "insert")
	{
		return prepareInsert(input, statement);
	}
	else if (input == "select")
	{
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void printRow(Row *row)
{
	cout << "(" << row->id << ", " << row->username << ", " << row->email << ")" << endl;
}

ExecuteResult executeInsert(Statement *statement, Table *table)
{
	if (table->numRows >= TABLE_MAX_ROWS)
	{
		return EXECUTE_TABLE_FULL;
	}

	Row *rowToInsert = &(statement->row);
	void *slot = rowSlot(table, table->numRows);
	serializeRow(rowToInsert, slot);
	table->numRows += 1;

	return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement *statement, Table *table)
{
	Row row;

	for (uint32_t i = 0; i < table->numRows; i++)
	{
		deserializeRow(rowSlot(table, i), &row);
		printRow(&row);
	}

	return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement *statement, Table *table)
{
	switch (statement->type)
	{
	case (STATEMENT_INSERT):
		return executeInsert(statement, table);
	case (STATEMENT_SELECT):
		return executeSelect(statement, table);
	}
}

int metaCommand(string command, Table *table)
{
	if (command.compare(".exit") == 0)
	{
		db_close(table);
		exit(0);
	}

	return META_COMMAND_UNRECOGNIZED_COMMAND;
}

int main(int argc, char *argv[])
{
	if (argc < 2)
	{
		printf("Must supply a database filename.\n");
		exit(EXIT_FAILURE);
	}

	char *filename = argv[1];
	Table *table = db_open(filename);

	while (true)
	{
		// Read input
		cout << "db > ";
		string input = "";
		getline(cin, input);

		// Handle meta commands
		if (input[0] == '.')
		{
			switch (metaCommand(input, table))
			{
			case (META_COMMAND_SUCCESS):
				continue;
			case (META_COMMAND_UNRECOGNIZED_COMMAND):
				cout << "Unrecognized command '" << input << "'" << endl;
				continue;
			}
		}

		// Create a statement
		Statement statement;
		switch (prepareStatement(input, &statement))
		{
		case (PREPARE_SUCCESS):
			break;
		case (PREPARE_SYNTAX_ERROR):
			cout << "Syntax error. Could not parse statement" << endl;
			continue;
		case (PREPARE_UNRECOGNIZED_STATEMENT):
			cout << "Unrecognized keyword at start of '" << input << "'" << endl;
			continue;
		case (PREPARE_STRING_TOO_LONG):
			cout << "String is too long." << endl;
			continue;
		case (PREPARE_NEGATIVE_ID):
			cout << "ID must be positive." << endl;
			continue;
		}

		switch (executeStatement(&statement, table))
		{
		case (EXECUTE_SUCCESS):
			cout << "Executed." << endl;
			break;
		case (EXECUTE_TABLE_FULL):
			cout << "Error: Table full" << endl;
			break;
		}
	}

	return 0;
}
