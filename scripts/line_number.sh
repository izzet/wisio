
SO_LIB=$1
FUNC=$2
OFFSET=$3
base_addr=`nm ${SO_LIB} | grep "\s${FUNC}$" | awk '{print $1}'`
hex_val=`python2 -c "print hex(0x${base_addr}+${OFFSET})"`
$ # use addr2line to get the line information, assuming any is available
line_number=`addr2line -e ${SO_LIB} ${hex_val}`
echo line_number