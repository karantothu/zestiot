const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);

const data = [];

io.on("connection", socket => {
    let previousId;
    const safeJoin = currentId => {
      socket.leave(previousId);
      socket.join(currentId);
      previousId = currentId;
    };
  
    socket.on("getData", latLonId => {
      safeJoin(latLonId);
      socket.emit("latLon", data);
    });
  
    socket.on("addData", latLon => {
      data.push(latLon);
      safeJoin(latLon.id);
      io.emit("data", data);
      socket.emit("data", data);
    });
  
    io.emit("data", data);
  });

http.listen(3000, () =>{ console.log("Listening at: 3000...")});