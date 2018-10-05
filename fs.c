#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "fs.h"
#include "fs_util.h"
#include "disk.h"

char inodeMap[MAX_INODE / 8];
char blockMap[MAX_BLOCK / 8];
Inode inode[MAX_INODE];
SuperBlock superBlock;
Dentry curDir;
int curDirBlock;

int fs_mount(char *name)
{
		int numInodeBlock =  (sizeof(Inode)*MAX_INODE)/ BLOCK_SIZE;
		int i, index, inode_index = 0;

		// load superblock, inodeMap, blockMap and inodes into the memory
		if(disk_mount(name) == 1) {
				disk_read(0, (char*) &superBlock);
				disk_read(1, inodeMap);
				disk_read(2, blockMap);
				for(i = 0; i < numInodeBlock; i++)
				{
						index = i+3;
						disk_read(index, (char*) (inode+inode_index));
						inode_index += (BLOCK_SIZE / sizeof(Inode));
				}
				// root directory
				curDirBlock = inode[0].directBlock[0];
				disk_read(curDirBlock, (char*)&curDir);

		} else {
		// Init file system superblock, inodeMap and blockMap
				superBlock.freeBlockCount = MAX_BLOCK - (1+1+1+numInodeBlock);
				superBlock.freeInodeCount = MAX_INODE;

				//Init inodeMap
				for(i = 0; i < MAX_INODE / 8; i++)
				{
						set_bit(inodeMap, i, 0);
				}
				//Init blockMap
				for(i = 0; i < MAX_BLOCK / 8; i++)
				{
						if(i < (1+1+1+numInodeBlock)) set_bit(blockMap, i, 1);
						else set_bit(blockMap, i, 0);
				}
				//Init root dir
				int rootInode = get_free_inode();
				curDirBlock = get_free_block();

				inode[rootInode].type =directory;
				inode[rootInode].owner = 0;
				inode[rootInode].group = 0;
				gettimeofday(&(inode[rootInode].created), NULL);
				gettimeofday(&(inode[rootInode].lastAccess), NULL);
				inode[rootInode].size = 1;
				inode[rootInode].blockCount = 1;
				inode[rootInode].directBlock[0] = curDirBlock;

				curDir.numEntry = 1;
				strncpy(curDir.dentry[0].name, ".", 1);
				curDir.dentry[0].name[1] = '\0';
				curDir.dentry[0].inode = rootInode;
				disk_write(curDirBlock, (char*)&curDir);
		}
		return 0;
}

int fs_umount(char *name)
{
		int numInodeBlock =  (sizeof(Inode)*MAX_INODE )/ BLOCK_SIZE;
		int i, index, inode_index = 0;
		disk_write(0, (char*) &superBlock);
		disk_write(1, inodeMap);
		disk_write(2, blockMap);
		for(i = 0; i < numInodeBlock; i++)
		{
				index = i+3;
				disk_write(index, (char*) (inode+inode_index));
				inode_index += (BLOCK_SIZE / sizeof(Inode));
		}
		// current directory
		disk_write(curDirBlock, (char*)&curDir);

		disk_umount(name);	
}

int search_cur_dir(char *name)
{
		// return inode. If not exist, return -1
		int i;

		for(i = 0; i < curDir.numEntry; i++)
		{
				if(command(name, curDir.dentry[i].name)) return curDir.dentry[i].inode;
		}
		return -1;
}

int remove_cur_dir_entry(char *name)
{
		int i, shift = 0;

		for(i = 0; i < curDir.numEntry; i++)
		{
				if(shift)
						curDir.dentry[i-1] = curDir.dentry[i];
				if(command(name, curDir.dentry[i].name))
						shift = 1;
		}

		if(shift)
		{
				curDir.numEntry--;
				return 0;
		}

		return -1;
}

int file_create(char *name, int size)
{
		int i;

		if(size >= LARGE_FILE) {
				printf("Do not support files larger than %d bytes yet.\n", LARGE_FILE);
				return -1;
		}

		int inodeNum = search_cur_dir(name); 
		if(inodeNum >= 0) {
				printf("File create failed:  %s exist.\n", name);
				return -1;
		}

		if(curDir.numEntry + 1 >= MAX_DIR_ENTRY) {
				printf("File create failed: directory is full!\n");
				return -1;
		}

		int numBlock = size / BLOCK_SIZE;
		if(size % BLOCK_SIZE > 0) numBlock++;

		int indirectBlockCount = numBlock > 10 ? 1 : 0;

		if(numBlock + indirectBlockCount > superBlock.freeBlockCount) {
				printf("File create failed: not enough space\n");
				return -1;
		}

		if(superBlock.freeInodeCount < 1) {
				printf("File create failed: not enough inode\n");
				return -1;
		}

		char *tmp = (char*) malloc(sizeof(char) * size+1);

		rand_string(tmp, size);
		printf("rand_string = %s\n", tmp);
		
		// get available inode and fill it
		inodeNum = get_free_inode();
		if(inodeNum < 0) {
				printf("File_create error: not enough inode.\n");
				return -1;
		}
		
		inode[inodeNum].type = file;
		inode[inodeNum].owner = 1;
		inode[inodeNum].group = 2;
		gettimeofday(&(inode[inodeNum].created), NULL);
		gettimeofday(&(inode[inodeNum].lastAccess), NULL);
		inode[inodeNum].size = size;
		inode[inodeNum].blockCount = numBlock;
		
		// add a new file into the current directory entry
		strncpy(curDir.dentry[curDir.numEntry].name, name, strlen(name));
		curDir.dentry[curDir.numEntry].name[strlen(name)] = '\0';
		curDir.dentry[curDir.numEntry].inode = inodeNum;
		printf("curdir %s, name %s\n", curDir.dentry[curDir.numEntry].name, name);
		curDir.numEntry++;

		// get data blocks
		int *indirectBlockArray;
		for(i = 0; i < numBlock; i++)
		{
				int block = get_free_block();
				if(block == -1) {
						printf("File_create error: get_free_block failed\n");
						return -1;
				}
				disk_write(block, tmp+(i*BLOCK_SIZE));
				if(i < 10)
						inode[inodeNum].directBlock[i] = block;
				else if(i == 10)
				{
						indirectBlockArray = (int *)malloc(BLOCK_SIZE);
						int blockindirect = get_free_block();
						inode[inodeNum].indirectBlock = blockindirect;
						indirectBlockArray[0] = block;
				}
				else
				{
					indirectBlockArray[i-10] = block;
				}
		}

		if(numBlock >= 10)
		{
			disk_write(inode[inodeNum].indirectBlock, (char *)indirectBlockArray);
			free(indirectBlockArray);
		}

		printf("file created: %s, inode %d, size %d\n", name, inodeNum, size);

		free(tmp);
		return 0;
}

int file_cat(char *name)
{
		int i;

		int inodeNum = search_cur_dir(name); 
		if(inodeNum < 0) {
				printf("File cat failed:  %s does not exist.\n", name);
				return -1;
		}

		int size = inode[inodeNum].size;

		if(size >= LARGE_FILE) {
				printf("Do not support files larger than %d bytes yet.\n", LARGE_FILE);
				return -1;
		}

		int numBlock = inode[inodeNum].blockCount;

		char *tmp = (char*) malloc(sizeof(char) * size+1);

		// get data blocks
		int *indirectBlockArray;

		for(i = 0; i < numBlock; i++)
		{
				int block;
				if(i < 10)
						block = inode[inodeNum].directBlock[i];
				else if(i == 10)
				{
						indirectBlockArray = (int *)malloc(BLOCK_SIZE);
						disk_read(inode[inodeNum].indirectBlock, (char *)indirectBlockArray);
						block = indirectBlockArray[0];
				}
				else
				{
					block = indirectBlockArray[i-10];
				}

				if(block == -1) {
						printf("File_read error: inode corrupted\n");
						return -1;
				}
				disk_read(block, tmp+(i*BLOCK_SIZE));
		}

		if(numBlock > 10)
		{
			free(indirectBlockArray);
		}

		tmp[size+1] = '\0';

		printf("%s file contents:\n%s \n", name, tmp);

		free(tmp);

		return 0;
}

int file_read(char *name, int offset, int size)
{
		int i;

		int inodeNum = search_cur_dir(name); 
		if(inodeNum < 0) {
				printf("File cat failed:  %s does not exist.\n", name);
				return -1;
		}

		int filesize = inode[inodeNum].size;

		if(filesize >= LARGE_FILE) {
				printf("Do not support files larger than %d bytes yet.\n", LARGE_FILE);
				return -1;
		}

		if(offset >= filesize)
		{
				printf("Cannot read. Offset is larger than filesize %d bytes.\n", filesize);
				return -1;
		}

		int numBlock = inode[inodeNum].blockCount;

		int firstBlock = offset / BLOCK_SIZE;
		int firstByteInBlock = offset % BLOCK_SIZE;
		int lastBlock = (offset + size - 1) / BLOCK_SIZE;
		int lastByteInBlock = (offset + size - 1) % BLOCK_SIZE;
		if(numBlock < lastBlock + 1)
		{
				lastBlock = numBlock - 1;
				lastByteInBlock = (filesize - 1) % BLOCK_SIZE;
		}

		char *tmp = (char*) malloc(sizeof(char) * (lastBlock - firstBlock + 1) * BLOCK_SIZE+1);

		// get data blocks
		int *indirectBlockArray;
		if(lastBlock >= 10)
		{
				indirectBlockArray = (int *)malloc(BLOCK_SIZE);
				disk_read(inode[inodeNum].indirectBlock, (char *)indirectBlockArray);
		}

		for(i = firstBlock; i <= lastBlock; i++)
		{
				int block;
				if(i < 10)
						block = inode[inodeNum].directBlock[i];
				else
				{
					block = indirectBlockArray[i-10];
				}

				if(block == -1) {
						printf("File_read error: inode corrupted\n");
						return -1;
				}
				disk_read(block, tmp+((i-firstBlock)*BLOCK_SIZE));
		}

		if(lastBlock >= 10)
		{
			free(indirectBlockArray);
		}

		tmp[(lastBlock - firstBlock) * BLOCK_SIZE + lastByteInBlock + 1] = '\0';

		printf("%s read contents:\n%s \n", name, tmp+firstByteInBlock);

		free(tmp);

		return 0;
}

int file_write(char *name, int offset, int size, char *buf)
{	
		int i;

		int inodeNum = search_cur_dir(name); 
		if(inodeNum < 0) {
				printf("File cat failed:  %s does not exist.\n", name);
				return -1;
		}

		int filesize = inode[inodeNum].size;

		if(filesize >= LARGE_FILE || offset + size >= LARGE_FILE) {
				printf("Do not support files larger than %d bytes yet.\n", LARGE_FILE);
				return -1;
		}

		if(offset > filesize)
		{
				printf("Cannot write. Offset is larger than filesize %d bytes.\n", filesize);
				return -1;
		}

		int numBlock = inode[inodeNum].blockCount;

		int firstBlock = offset / BLOCK_SIZE;
		int firstByteInBlock = offset % BLOCK_SIZE;
		int lastBlock = (offset + size - 1) / BLOCK_SIZE;
		int lastByteInBlock = (offset + size - 1) % BLOCK_SIZE;

		int indirectBlockCount = (lastBlock + 1) > 10 && numBlock <= 10 ? 1 : 0;

		if(lastBlock + 1 - numBlock + indirectBlockCount > superBlock.freeBlockCount) {
				printf("File write failed: not enough space\n");
				return -1;
		}

		if(offset + size + 1 > filesize)
		{
				inode[inodeNum].blockCount = lastBlock + 1;
				inode[inodeNum].size = offset + size + 1;
		}

		int bufoffset = 0;

		int *indirectBlockArray;
		if(lastBlock + 1 > 10)
		{
				indirectBlockArray = (int *)malloc(BLOCK_SIZE);
				if(numBlock > 10)
				{
						disk_read(inode[inodeNum].indirectBlock, (char *)indirectBlockArray);
				}
				else
				{
						int block = get_free_block();
						if(block < 0)
						{
								printf("error allocating indirect block. failed writing\n");
								return -1;
						}
						inode[inodeNum].indirectBlock = block;
				}
		}

		for(i = firstBlock; i <= lastBlock; i++)
		{
				if(i == firstBlock && firstBlock < numBlock)
				{
						int block;

						if(i < 10)
								block = inode[inodeNum].directBlock[i];
						else
								block = indirectBlockArray[i-10];

						char *tmp = (char*) malloc(sizeof(char) * BLOCK_SIZE);
						disk_read(block, tmp);

						int lastByte = BLOCK_SIZE;
						if(lastBlock == firstBlock)
						{
							lastByte = lastByteInBlock;
						}

						memcpy(tmp+firstByteInBlock, buf, lastByte-firstByteInBlock+1);

						disk_write(block,tmp);
						free(tmp);

						bufoffset = lastByte-firstByteInBlock+1;
				}
				else
				{
						int block;
						if(i < numBlock && i < 10)
						{
								block = inode[inodeNum].directBlock[i];
						}
						else if(i < numBlock && i >= 10)
						{
								block = indirectBlockArray[i-10];
						}
						else
						{
								block = get_free_block();
								if(i < 10)
										inode[inodeNum].directBlock[i] = block;
								else
										indirectBlockArray[i-10] = block;
						}

						if(block == -1) {
								printf("File_read error: inode corrupted\n");
								return -1;
						}

						disk_write(block, buf+((i-firstBlock)*BLOCK_SIZE)+bufoffset);
				}
		}

		if(lastBlock + 1 > 10)
		{
				disk_write(inode[inodeNum].indirectBlock, (char *)indirectBlockArray);
				free(indirectBlockArray);
		}
		return 0;
}

int file_stat(char *name)
{
		char timebuf[28];
		int inodeNum = search_cur_dir(name);
		if(inodeNum < 0) {
				printf("file cat error: file is not exist.\n");
				return -1;
		}

		printf("Inode = %d\n", inodeNum);
		if(inode[inodeNum].type == file) printf("type = file\n");
		else printf("type = directory\n");
		printf("owner = %d\n", inode[inodeNum].owner);
		printf("group = %d\n", inode[inodeNum].group);
		printf("size = %d\n", inode[inodeNum].size);
		printf("num of block = %d\n", inode[inodeNum].blockCount);
		format_timeval(&(inode[inodeNum].created), timebuf, 28);
		printf("Created time = %s\n", timebuf);
		format_timeval(&(inode[inodeNum].lastAccess), timebuf, 28);
		printf("Last accessed time = %s\n", timebuf);
}

int file_remove(char *name)
{
		int i;

		int inodeNum = search_cur_dir(name); 
		if(inodeNum < 0) {
				printf("File delete failed:  %s does not exist.\n", name);
				return -1;
		}
		
		int size = inode[inodeNum].size;

		if(size >= LARGE_FILE) {
				printf("Do not support files larger than %d bytes yet.\n", LARGE_FILE);
				return -1;
		}

		int numBlock = inode[inodeNum].blockCount;

		int *indirectBlockArray;
		for(i = 0; i < numBlock; i++)
		{
				int block;
				if(i < 10)
						block = inode[inodeNum].directBlock[i];
				else if(i == 10)
				{
						indirectBlockArray = (int *)malloc(BLOCK_SIZE);
						disk_read(inode[inodeNum].indirectBlock, (char *)indirectBlockArray);
						block = indirectBlockArray[0];
				}
				else
				{
					block = indirectBlockArray[i-10];
				}

				if(block == -1) {
						printf("File_delete error: block corrupted\n");
						return -1;
				}
				free_block(block);
		}

		if(numBlock > 10)
		{
			free_block(inode[inodeNum].indirectBlock);
			free(indirectBlockArray);
		}

		remove_cur_dir_entry(name);
		free_inode(inodeNum);		

		int curDirInode = search_cur_dir(".");
		int curDirBlock = inode[curDirInode].directBlock[0];

		disk_write(curDirBlock, (char*)&curDir);

		printf("file removed: %s, inode %d, size %d\n", name, inodeNum, size);

		return 0;
}

int dir_make(char* name)
{
		if(curDir.numEntry + 1 >= MAX_DIR_ENTRY)
		{
				printf("Current Directory Full. No more entries possible\n");
				return -1;
		}
		
		if(superBlock.freeBlockCount < 1)
		{
				printf("no free blocks available. Directory creation not possible\n");
				return -1;
		}

		if(superBlock.freeInodeCount < 1)
		{
				printf("no free inodes available. Directory creation not possible\n");
				return -1;
		}

		int newDirInode = get_free_inode();
		int newDirBlock = get_free_block();
		int curDirInode = search_cur_dir(".");
		int curDirBlock = inode[curDirInode].directBlock[0];

		inode[newDirInode].type =directory;
		inode[newDirInode].owner = 0;
		inode[newDirInode].group = 0;
		gettimeofday(&(inode[newDirInode].created), NULL);
		gettimeofday(&(inode[newDirInode].lastAccess), NULL);
		inode[newDirInode].size = 1;
		inode[newDirInode].blockCount = 1;
		inode[newDirInode].directBlock[0] = newDirBlock;

		Dentry newDir;
		newDir.numEntry = 2;
		strncpy(newDir.dentry[0].name, ".", 1);
		newDir.dentry[0].name[1] = '\0';
		newDir.dentry[0].inode = newDirInode;
		strncpy(newDir.dentry[1].name, "..", 2);
		newDir.dentry[1].name[2] = '\0';
		newDir.dentry[1].inode = curDirInode;

		disk_write(newDirBlock, (char*)&newDir);


		curDir.numEntry += 1;
		strncpy(curDir.dentry[curDir.numEntry - 1].name, name, strlen(name));
		curDir.dentry[curDir.numEntry - 1].inode = newDirInode;
		disk_write(curDirBlock, (char*)&curDir);

		return 0;
}

int dir_remove(char *name)
{

		int delDirInode = search_cur_dir(name);

		if(delDirInode < 0)
		{
				printf("directory does not exist\n");
				return -1;
		}

		int delDirBlock = inode[delDirInode].directBlock[0];
		Dentry delDir;
		disk_read(delDirBlock, (char*)&delDir);

		if(delDir.numEntry > 2)
		{
				printf("directory has either subdirectory or files. Cannot be deleted\n");
				return -1;
		}

		int curDirInode = search_cur_dir(".");
		int curDirBlock = inode[curDirInode].directBlock[0];

		remove_cur_dir_entry(name);
		disk_write(curDirBlock, (char*)&curDir);

		free_inode(delDirInode);		
		free_block(delDirBlock);

		return 0;
}

int dir_change(char* name)
{
		int chDirInode = search_cur_dir(name);

		if(chDirInode < 0)
		{
				printf("directory does not exist\n");
				return -1;
		}

		int chDirBlock = inode[chDirInode].directBlock[0];

		disk_read(chDirBlock, (char *)&curDir);
}

int ls()
{
		int i;
		for(i = 0; i < curDir.numEntry; i++)
		{
				int n = curDir.dentry[i].inode;
				if(inode[n].type == file) printf("type: file, ");
				else printf("type: dir, ");
				printf("name \"%s\", inode %d, size %d byte\n", curDir.dentry[i].name, curDir.dentry[i].inode, inode[n].size);
		}

		return 0;
}

int fs_stat()
{
		printf("File System Status: \n");
		printf("# of free blocks: %d (%d bytes), # of free inodes: %d\n", superBlock.freeBlockCount, superBlock.freeBlockCount*512, superBlock.freeInodeCount);
}

int execute_command(char *comm, char *arg1, char *arg2, char *arg3, char *arg4, int numArg)
{
		if(command(comm, "create")) {
				if(numArg < 2) {
						printf("error: create <filename> <size>\n");
						return -1;
				}
				return file_create(arg1, atoi(arg2)); // (filename, size)
		} else if(command(comm, "cat")) {
				if(numArg < 1) {
						printf("error: cat <filename>\n");
						return -1;
				}
				return file_cat(arg1); // file_cat(filename)
		} else if(command(comm, "write")) {
				if(numArg < 4) {
						printf("error: write <filename> <offset> <size> <buf>\n");
						return -1;
				}
				return file_write(arg1, atoi(arg2), atoi(arg3), arg4); // file_write(filename, offset, size, buf);
		}	else if(command(comm, "read")) {
				if(numArg < 3) {
						printf("error: read <filename> <offset> <size>\n");
						return -1;
				}
				return file_read(arg1, atoi(arg2), atoi(arg3)); // file_read(filename, offset, size);
		} else if(command(comm, "rm")) {
				if(numArg < 1) {
						printf("error: rm <filename>\n");
						return -1;
				}
				return file_remove(arg1); //(filename)
		} else if(command(comm, "mkdir")) {
				if(numArg < 1) {
						printf("error: mkdir <dirname>\n");
						return -1;
				}
				return dir_make(arg1); // (dirname)
		} else if(command(comm, "rmdir")) {
				if(numArg < 1) {
						printf("error: rmdir <dirname>\n");
						return -1;
				}
				return dir_remove(arg1); // (dirname)
		} else if(command(comm, "cd")) {
				if(numArg < 1) {
						printf("error: cd <dirname>\n");
						return -1;
				}
				return dir_change(arg1); // (dirname)
		} else if(command(comm, "ls"))  {
				return ls();
		} else if(command(comm, "stat")) {
				if(numArg < 1) {
						printf("error: stat <filename>\n");
						return -1;
				}
				return file_stat(arg1); //(filename)
		} else if(command(comm, "df")) {
				return fs_stat();
		} else {
				fprintf(stderr, "%s: command not found.\n", comm);
				return -1;
		}
		return 0;
}

