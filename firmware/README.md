To send the command from your Ubuntu server via the terminal, first configure the baud rate for the USB serial port (usually /dev/ttyUSB0 for a Nano):
Bash
 
Plain Text
stty -F /dev/ttyUSB0 9600
Then, send the command to turn the LED on:
Bash
 
Plain Text
echo -n "1" > /dev/ttyUSB0
To turn it off, send 0:
Bash
 
Plain Text
echo -n "0" > /dev/ttyUSB0