import socket,threading
from tkinter import *
import tkinter.messagebox as messagebox
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 9999))
s.listen(5)
print ('Waiting for connection...')

def tcplink(sock, addr):
    print ('Accept new connection from %s:%s...' % addr)
    sock.send(b'Welcome!')
    while True:
        while True:
            text=input("Please enter: ")
            if (text=="exit"):
                break
            sock.send(text.encode())
        data = sock.recv(1024)
        time.sleep(1)
        if not data or data.decode('utf-8') == 'exit':
            break
        sock.send(('Hello, %s!' % data.decode('utf-8')).encode('utf-8'))
    sock.close()
    print ('Connection from %s:%s closed.' % addr)

# class Application(Frame):
#     def __init__(self, master=None):
#         Frame.__init__(self, master)
#         self.pack()
#         self.createWidgets()
#
#     def createWidgets(self):
#         self.nameInput = Entry(self)
#         self.nameInput.pack()
#         self.alertButton = Button(self, text='Hello', command=self.hello)
#         self.alertButton.pack()
#
#     def hello(self):
#         name = self.nameInput.get() or 'world'
#         messagebox.showinfo('Message', 'Hello, %s' % name)

while True:
    sock, addr = s.accept()
    t = threading.Thread(target=tcplink, args=(sock, addr))
    t.start()
    # app = Application()
    # app.master.title('Hello World')
    # app.mainloop()
