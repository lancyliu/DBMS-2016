buffer_mgr: storage_mgr.o dberror.o test_assign2_1.o buffer_mgr.o buffer_mgr_stat.o
	gcc -o $@ $^  
storage_mgr.o: storage_mgr.c dberror.h storage_mgr.h  
	gcc -c $<  
dberror.o: dberror.c dberror.h  
	gcc -c $<  
test_assign2_1.o: test_assign2_1.c storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h
	gcc -c $< 
buffer_mgr.o: buffer_mgr.c buffer_mgr.h storage_mgr.h
	gcc -c $<
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	gcc -c $<
	: clean
clean:
	-rm *.o buffer_mgr