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

enum NodeType
{
	NODE_INTERNAL,
	NODE_LEAF
};

/**
 * Common Node Header Layout 
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/**
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

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

/**
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

struct Pager
{
	int file_descriptor;
	uint32_t file_length;
	uint32_t numPages;
	void *pages[TABLE_MAX_PAGES];
};

struct Table
{
	Pager *pager;
	uint32_t root_page_num;
};

struct Cursor
{
	Table *table;
	uint32_t page_num;
	uint32_t cell_num;
	bool endOfTable; // Indicates a position one past the last element
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

		if (page_num >= pager->numPages)
		{
			pager->numPages += 1;
		}
	}

	return pager->pages[page_num];
}

uint32_t *leaf_node_num_cells(void *node)
{
	return (uint32_t *)node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leaf_node_cell(void *node, uint32_t cell_num)
{
	return (char *)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t *leaf_node_key(void *node, uint32_t cell_num)
{
	return (uint32_t *)leaf_node_cell(node, cell_num);
}

void *leaf_node_value(void *node, uint32_t cell_num)
{
	return (char *)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void *node) { *leaf_node_num_cells(node) = 0; }

Cursor *tableStart(Table *table)
{
	Cursor *cursor = new Cursor();
	cursor->table = table;
	cursor->page_num = table->root_page_num;
	cursor->cell_num = 0;

	void *root_node = get_page(table->pager, table->root_page_num);
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	cursor->endOfTable = (num_cells == 0);

	return cursor;
}

Cursor *tableEnd(Table *table)
{
	Cursor *cursor = new Cursor();
	cursor->table = table;
	cursor->page_num = table->root_page_num;
	cursor->endOfTable = true;

	void *root_node = get_page(table->pager, table->root_page_num);
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	cursor->cell_num = num_cells;
	cursor->endOfTable = true;

	return cursor;
}

void pager_flush(Pager *pager, uint32_t page_num)
{
	if (pager->pages[page_num] == NULL)
	{
		cout << "Tried to flush null page" << endl;
		exit(EXIT_FAILURE);
	}

	off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

	if (offset == -1)
	{
		printf("Error seeking: %d\n", errno);
		exit(EXIT_FAILURE);
	}

	ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

	if (bytes_written == -1)
	{
		printf("Error writing: %d\n", errno);
		exit(EXIT_FAILURE);
	}
}

void db_close(Table *table)
{
	Pager *pager = table->pager;

	for (uint32_t i = 0; i < pager->numPages; i++)
	{
		if (pager->pages[i] == NULL)
		{
			continue;
		}
		pager_flush(pager, i);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	int result = close(pager->file_descriptor);
	if (result == -1)
	{
		cout << "Error closing db file." << endl;
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
	pager->numPages = (file_length / PAGE_SIZE);

	if (file_length % PAGE_SIZE != 0)
	{
		printf("Db file is not a whole number of pages. Corrupt file.\n");
		exit(EXIT_FAILURE);
	}

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

	if (pager->numPages == 0)
	{
		/**
		 * New database file.
		 * Initialize page 0 as leaf node.
		 */
		void *root_node = get_page(pager, 0);
		initialize_leaf_node(root_node);
	}

	return table;
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

void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value)
{
	void *node = get_page(cursor->table->pager, cursor->page_num);

	uint32_t num_cells = *leaf_node_num_cells(node);
	if (num_cells >= LEAF_NODE_MAX_CELLS)
	{
		// Node full
		printf("Need to implement splitting a leaf node \n");
		exit(EXIT_FAILURE);
	}

	if (cursor->cell_num < num_cells)
	{
		// Make room for new cell
		for (uint32_t i = num_cells; i > cursor->cell_num; i--)
		{
			memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
		}
	}

	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, cursor->cell_num)) = key;
	serializeRow(value, leaf_node_value(node, cursor->cell_num));
}

void *cursorValue(Cursor *cursor)
{
	uint32_t page_num = cursor->page_num;
	void *page = get_page(cursor->table->pager, page_num);

	return leaf_node_value(page, cursor->cell_num);
}

void cursorAdvance(Cursor *cursor)
{
	uint32_t page_num = cursor->page_num;
	void *node = get_page(cursor->table->pager, page_num);

	cursor->cell_num += 1;

	if (cursor->cell_num >= (*leaf_node_num_cells(node)))
	{
		cursor->endOfTable = true;
	}
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

void print_leaf_node(void *node)
{
	uint32_t num_cells = *leaf_node_num_cells(node);
	printf("leaf (size %d)\n", num_cells);
	for (uint32_t i = 0; i < num_cells; i++)
	{
		uint32_t key = *leaf_node_key(node, i);
		printf("  - %d : %d\n", i, key);
	}
}

void print_constants()
{
	printf("ROW_SIZE: %d\n", ROW_SIZE);
	printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
	printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
	printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
	printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
	printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

ExecuteResult executeInsert(Statement *statement, Table *table)
{
	void *node = get_page(table->pager, table->root_page_num);
	if ((*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS))
	{
		return EXECUTE_TABLE_FULL;
	}

	Row *rowToInsert = &(statement->row);
	Cursor *cursor = tableEnd(table);

	leaf_node_insert(cursor, rowToInsert->id, rowToInsert);

	delete cursor;

	return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement *statement, Table *table)
{
	Cursor *cursor = tableStart(table);
	Row row;

	while (!(cursor->endOfTable))
	{
		deserializeRow(cursorValue(cursor), &row);
		printRow(&row);
		cursorAdvance(cursor);
	}

	delete cursor;

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
	else if (command.compare(".constants") == 0)
	{
		printf("Constants:\n");
		print_constants();
		return META_COMMAND_SUCCESS;
	}
	else if (command.compare(".btree") == 0)
	{
		printf("Tree:\n");
		print_leaf_node(get_page(table->pager, 0));
		return META_COMMAND_SUCCESS;
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
