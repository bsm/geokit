/*
Package cellstore contains a toolkit for building fast and efficient
"write-once, read-only" proximity indices.

Data Structure Documentation

Store

A store contains a series of data blocks followed by an index and
a store footer.

    Store layout:
    +---------+---------+---------+-------------+--------------+
    | block 1 |   ...   | block n | block index | store footer |
    +---------+---------+---------+-------------+--------------+

    Block index:
    +----------------------------+--------------------+----------------------------------+--------------------------+--------+
    | last cell block 1 (varint) |  offset 2 (varint) | last cell block 2 (varint,delta) |  offset 2 (varint,delta) |   ...  |
    +----------------------------+--------------------+----------------------------------+--------------------------+--------+

    Store footer:
    +------------------------+------------------+
    | index offset (8 bytes) |  magic (8 bytes) |
    +------------------------+------------------+

Block

A block comprises of a series of sections, followed by a section
index and a single-byte compression type indicator.

    Block layout:
    +-----------+---------+-----------+---------------+---------------------------+
    | section 1 |   ...   | section n | section index | compression type (1-byte) |
    +-----------+---------+-----------+---------------+---------------------------+

    Section index:
    +----------------------------+-------+----------------------------+-------------------------------+
    | section offset 1 (4 bytes) |  ...  | section offset n (4 bytes) |  number of sections (4 bytes) |
    +----------------------------+-------+----------------------------+-------------------------------+

Section

A section is a series of (s2) cellID-value pairs (= entries) where the cell ID of the
first entry is stored as a full uint64 while the cell IDs of all subsequent entries
are delta encoded.

    +-----------------+----------------------+------------------+-----------------------+----------------------+------------------+-------+
    | cell 1 (varint) | value len 1 (varint) | value 1 (varlen) | cell 2 (varint,delta) | value len 2 (varint) | value 2 (varlen) |  ...  |
    +-----------------+----------------------+------------------+-----------------------+----------------------+------------------+-------+

*/
package cellstore
