#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#include <sqlite3.h>
#include "md5.h"
#include "lpc.h"

#define SEG_SIZE 4096
#define SEG_NR 1024
#define CTR_SIZE SEG_NR*(32+SEG_SIZE)
#define CTX_NR SEG_NR
#define CTX_SIZE CTX_NR*42

typedef struct _BAKEUP_CTX {
	char header[1030];
	char context[CTX_SIZE+1];
	int cp;
}BAK_CTX;

typedef struct _CONTAINER {
	char container[CTR_SIZE];
	int cp, cid;
}CTR;

BAK_CTX ctx;
CTR ctr;

void init() {
	memset(ctx.header, 0, sizeof(ctx.header));
	memset(ctx.context, 0, sizeof(ctx.context));
	ctx.cp = 0;
	memset(ctr.container, 0, sizeof(ctr.container));
	ctr.cp = ctr.cid = 0;
}

int callback (void *arg, int nr, char **value, char **name) {
	int *ret = (int*)arg;
	*ret = atoi(value[0]);
	return 0;
}

int lookup(char *md5) {
	static int flag = 0;
	sqlite3 *db;
	char sql[300];
	int cid;
	if (!flag) {
		flag = 1;
		sqlite3_open("./index.db", &db);
		sprintf(sql, "create table tb(md5 TEXT PRIMARY KEY, cid INTEGER)");
		sqlite3_exec(db, sql, NULL, NULL, NULL);
		sprintf(sql, "delete from tb");
		sqlite3_exec(db, sql, NULL, NULL, NULL);
	}
	sprintf(sql, "select cid from tb where md5=\"%s\"", md5);
	cid = -1;
	sqlite3_open("./index.db", &db);
	sqlite3_exec(db, sql, callback, &cid, NULL);
	sqlite3_close(db);
	return cid;
}

int insert(char *md5, char *buf) {
	sqlite3 *db;
	char sql[300], path[300];
	int ret, fdc;
	memcpy(&(ctr.container[32*ctr.cp]), md5, 32);
	memcpy(&(ctr.container[32*SEG_NR+SEG_SIZE*ctr.cp]), buf, SEG_SIZE);
	ctr.cp++;
	sprintf(sql, "insert into tb values(\"%s\", \"%d\")", md5, ctr.cid);
	sqlite3_open("./index.db", &db);
	sqlite3_exec(db, sql, NULL, NULL, NULL);
	sqlite3_close(db);
	ret = ctr.cid;
	if (ctr.cp == SEG_NR) {
		sprintf(path, "/home/hxs139/workspace/data/c%d", ctr.cid);
		fdc = open(path, O_RDWR | O_CREAT | O_TRUNC, 0777);
		write(fdc, ctr.container, CTR_SIZE);
		close(fdc);
		memset(ctr.container, 0, CTR_SIZE);
		ctr.cp = 0;
		ctr.cid++;
	}
	return ret;
}

void replace(int cid) {
	int fdc;
	char path[300], buf[32*SEG_NR+1] = {0};
	sprintf(path, "/home/hxs139/workspace/data/c%d", cid);
	fdc = open(path, O_RDONLY);
	read(fdc, buf, 32*SEG_NR);
	close(fdc);
	LPCReplace(cid, buf);
	return;
}

int split(int fd, int bak) {
	char *buf = NULL, *md5, str[32+1+10];
	int rwsize = 0, cid = -1, cnt = 0;
	int ret = 0;
	if (NULL == (buf = (char*)malloc(SEG_SIZE+1))) {
		perror("malloc buf for read");
		ret = errno;
		goto _SPLIT_EXIT;
	}
	memset(buf, 0, SEG_SIZE+1);
	while (rwsize = read(fd, buf, SEG_SIZE)) {
		md5 = MD5String(buf);
		if (-1 == (cid = LPCHit(md5))) {
			if (-1 == (cid = lookup(md5))) {
				cid = insert(md5, buf);
			} else {
				replace(cid);
			}	
		}
		memset(str, 0, sizeof(str));
		sprintf(str, "%s,%d\n", md5, cid);
		memcpy(&(ctx.context[ctx.cp]), str, strlen(str));
		ctx.cp += strlen(str);
		cnt++;
		if (cnt == CTX_NR) {
			write(bak, ctx.context, strlen(ctx.context));
			memset(ctx.context, 0, sizeof(ctx.context));
			ctx.cp = cnt = 0;
		}
		memset(buf, 0, sizeof(buf));
		cid = -1;
	}
	if (cnt) {
		write(bak, ctx.context, strlen(ctx.context));
		memset(ctx.context, 0, sizeof(ctx.context));
		ctx.cp = cnt = 0;
	}

_SPLIT_EXIT:
	if (buf)	free(buf);
	return ret;
}

int dedup_reg(char *fullpath, int bak) {
	int fd;
	int ret = 0;
	char str[1000];
	struct stat statbuf;
	printf("%s\n", fullpath);
	if (-1 == (fd = open(fullpath, O_RDONLY))) {
		perror("open regular file");
		ret = errno;
		goto _DEDUP_REG_EXIT;
	}
	lstat(fullpath, &statbuf);
	sprintf(ctx.header, "%s,%d\n", fullpath, (int)(statbuf.st_size+SEG_SIZE-1)/SEG_SIZE);
	write(bak, ctx.header, strlen(ctx.header));
	memset(ctx.header, 0, sizeof(ctx.header));
	ret |= split(fd, bak);

_DEDUP_REG_EXIT:
	close(fd);
	return ret;
}

int dedup_dir(char *root, int bak) {
	DIR *dir;
	struct dirent *dp;
	struct stat statbuf;
	char path[1000];
	int ret = 0;
	dir = opendir(root);
	if (dir == NULL) {
		perror("fail to open dir");
		ret = errno;
		goto _DEDUP_DIR_EXIT;
	}
	while ((dp = readdir(dir)) != NULL) {
		if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
			continue;
		if (strcmp(dp->d_name, "workspace") == 0)
			continue;
		sprintf(path, "%s/%s", root, dp->d_name);
		if (lstat(path, &statbuf) < 0) {
			perror("lstat source path");
			ret = errno;
			goto _DEDUP_DIR_EXIT;
		}
		if (S_ISDIR(statbuf.st_mode))
			ret |= dedup_dir(path, bak);
		else if (S_ISREG(statbuf.st_mode))
			ret |= dedup_reg(path, bak);
	}

_DEDUP_DIR_EXIT:
	closedir(dir);
	return ret;
}

void final() {
	char path[300];
	int fdc;
	sprintf(path, "/home/hxs139/workspace/data/c%d", ctr.cid);
	fdc = open(path, O_RDWR | O_CREAT | O_TRUNC, 0777);
	write(fdc, ctr.container, CTR_SIZE);
	close(fdc);
}

int main(int argc, char *argv[]) {
	int bak;
	int ret;
	char root[] = "/home/hxs139";
	init();
	LPCInit();
	if (-1 == (bak = open("./hxs139.bak", O_RDWR | O_CREAT | O_TRUNC, 0777))) {
		perror("open backup file");
		ret = errno;
		goto _MAIN_EXIT;
	}
	ret = dedup_dir(root, bak);
	if (ctr.cp)
		final();
	
_MAIN_EXIT:
	close(bak);
	return ret;
}
