#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <unistd.h>
#include "lpc.h"

typedef unsigned short int UINT2;
typedef unsigned long int UINT4;

#define LPC_SIZE 3000
#define SEG_NR 1024

typedef struct _CACHE_LINE {
	unsigned char md5[SEG_NR][16+1];
	UINT4 cid, t_stamp;
}LINE;

typedef struct _LOCALITY_PRESERVED_CACHE {
	LINE cache[LPC_SIZE];
}LPC;

LPC lpc;
static UINT4 stmp = 0;
int lru = 0;

int xtoi(char a, char b) {
	int ret = 0;
	if (isalpha(a))	ret += 16*(a-87);
	else			ret += 16*(a-48);
	if (isalpha(b))	ret += b-87;
	else			ret += b-48;
	return ret;
}

int cmp(const void *a, const void *b) {
	unsigned char *sa = (unsigned char*)a;
	unsigned char *sb = (unsigned char*)b;
	return strcmp(sa, sb) <= 0;
}

int b_search(unsigned char x[17], LINE ln) {
	int l = 0, r = SEG_NR-1, mid;
	int cp;
	while (l <= r) {
		mid = (l+r)>>1;
		cp = strcmp(ln.md5[mid], x);
		if (cp == 0)	return 1;
		if (cp < 0)		r = mid-1;
		if (cp > 0)		l = mid+1;
	}
	return 0;
}

void LPCInit () {
   int i;
   for (i = 0; i < LPC_SIZE; i++) {
	   lpc.cache[i].cid = -1;
	   lpc.cache[i].t_stamp = 0;
	   memset(lpc.cache[i].md5, 0, sizeof(lpc.cache[i].md5));
   }
}

int LPCHit(char *md5str) {
	unsigned char dig[17];
	int i, f;
	UINT4 mstmp = stmp;
	lru = 0;
	memset(dig, 0, sizeof(dig));
	for (i = 0; i < 16; i++)
		dig[i] = (unsigned char)xtoi(md5str[2*i], md5str[2*i+1]);
	for (i = 0; i < LPC_SIZE; i++) {
		f = b_search(dig, lpc.cache[i]);
		if (f) {
			lpc.cache[i].t_stamp = ++stmp;
			return lpc.cache[i].cid;
		} else {
			if (lpc.cache[i].t_stamp < mstmp) {
				mstmp = lpc.cache[i].t_stamp;
				lru = i;
			}
		}
	}
	return -1;
}

void LPCReplace (int cid, char *m_str) {
	int i, j;
	memset(lpc.cache[lru].md5, 0, sizeof(lpc.cache[lru].md5));
	lpc.cache[lru].cid = cid;
	lpc.cache[lru].t_stamp = ++stmp;
	for (j = 0; j < SEG_NR; j++) {
		for (i = 0; i < 16; i++)
			lpc.cache[lru].md5[j][i] = (unsigned char)xtoi(m_str[32*j+2*i], m_str[32*j+2*i+1]);
	}
	qsort(lpc.cache[lru].md5, SEG_NR, sizeof(lpc.cache[lru].md5[0]), cmp);
	return;
}
