#ifndef statRateCal__h
#define statRateCal__h

typedef struct st_rateCal {
	
	char _dbName[16];
	char _dbUser[16];
	char _dbPassword[16];
	char _terminal[16];

	int _totalNumDtoD;
	int _qualNumDtoD;
	int _groupNumDtoD;
	int _groupQualNumDtoD;

	char _qualifiedRateDtoD[16];
	char _groupHalfRateDtoD[16];

	int _totalNumDtoI;
	int _qualNumDtoI;
	int _groupNumDtoI;
	int _groupQualNumDtoI;

	char _qualifiedRateDtoI[16];
	char _groupHalfRateDtoI[16];

	char _startDate[16];
	char _endDate[16];

} Res_RateCalu;

#endif
