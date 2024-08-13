import React, { useState } from 'react';
import './App.css';
const uuid = require('uuid');

function App() {
  const [image, setImage] = useState(null);
  const [uploadResultMessage, setUploadResultMessage] = useState('Please upload the image to authenticate the employee');
  const [imgURL, setImgURL] = useState('');
  const [isAuth, setAuth] = useState(false);

  async function sendImage(e) {
    e.preventDefault();
    const visitorImageName = uuid.v4();
    
    try {
      await fetch(`https://qeyy01oop8.execute-api.us-east-1.amazonaws.com/dev/facial--visitor--images/${visitorImageName}.jpeg`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'image/jpeg'
        },
        body: image
      });

      const response = await authenticate(visitorImageName);
      if (response.Message === 'Success') {
        setAuth(true);
        setUploadResultMessage(`Hello, ${response.firstName} ${response.lastName}, have a great day.`);
      } else {
        setAuth(false);
        setUploadResultMessage('Authentication Failed.');
      }
    } catch (error) {
      setAuth(false);
      setUploadResultMessage('There is an error during the authentication process.');
      console.error(error);
    }
  }

  async function authenticate(visitorImageName) {
    const requestUrl = `https://qeyy01oop8.execute-api.us-east-1.amazonaws.com/dev/employee?${new URLSearchParams({
      objectKey: `${visitorImageName}.jpeg`
    })}`;

    try {
      const response = await fetch(requestUrl, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        }
      });
      const data = await response.json();
      return data;
    } catch (error) {
      console.error(error);
      return { Message: 'Error' };
    }
  }

  return (
    <div className="App">
      <h1>ABCD Enterprises Ltd.</h1>
      <h2>Employee Facial Recognition System</h2>
      <form onSubmit={sendImage}>
        <input type="file" name="image" onChange={e => {
          setImage(e.target.files[0]);
          setImgURL(URL.createObjectURL(e.target.files[0]));
        }} />
        <button type="submit">Authenticate the Employee</button>
        <div className={isAuth ? 'success' : 'failure'}>{uploadResultMessage}</div>
        {imgURL && <img src={imgURL} alt="Visitor" height={250} width={250} />}
      </form>
    </div>
  );
}

export default App;
