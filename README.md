# Task for VirtusLab Big Data internship.

The task was to implement a program, which joins two csv files, keeping in mind that files used could be much larger 
than available memory. No external libraries to use in a join function.

#### Idea:
For a clear demonstration of my thinking I decided to use python, even though it can be painfully slow without heavily 
optimized libraries written in C or C++.

I decided to try to implement a Block Nested Loop Join algorithm.

#### Execution:
My program consists of:
<ul>
    <li>
    __main__.py  -  initializer and argument parser
    </li>
    <li>
    join.py  -  main joining function
    </li>
    <li>
    utils.py  -  various functions used in the program
    </li>
</ul>

Usage (from the root of the project):
```
python . <file1> <file2> <join_col> <join_type> [--max_mem <max_mem>] 
```
You can specify maximum memory usage in megabytes using `--max_mem` option.


#### Testing:
In `test.py` you can generate various input files, test the program compare the output with the expected output 
against pandas implementation of the join algorithm. I tested out the program on files varying in size from few KB 
to several GB (with such large files it gets painfully slow, just as expected), with unique and non-unique 
join columns, and various join types. So far everything seems to be working fine.

However, I came across a problem in monitoring memory usage. I tried to control the memory usage of the program by 
automatically setting the block size, but it seems not to work correctly sometimes, when memory constraint is very low.
I suspect it's the way python works - and I should be grateful for the memory managing mechanisms that I do not have 
to do all of it manually on a daily basis.


#### Conclusion:
The program works correctly, but can be very slow. If I was to implement it in C or C++, It would be much more demanding 
task, but it would work much faster. 
