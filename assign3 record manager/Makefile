
all: test_assign3 test_expr

test_assign3: test_assign3_1.o expr.o dberror.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
			  cc -o test_assign3 test_assign3_1.o expr.o dberror.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o

test_expr:  test_expr.o expr.o dberror.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
			cc -o test_expr test_expr.o expr.o dberror.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h tables.h
			 cc -c test_expr.c

  :clean
  
           clean :
		     -rm *.o test_assign3 test_expr