import http from 'k6/http';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '20s', target: 10 },
    { duration: '20s', target: 100 },
    { duration: '20s', target: 500 },
  ],
};

function generateRandomString(size) {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let randomString = '';

  for (let i = 0; i < size; i++) {
    const randomIndex = Math.floor(Math.random() * characters.length);
    randomString += characters.charAt(randomIndex);
  }

  return randomString;
}

export default function() {
  const key = generateRandomString(20)
  const value = generateRandomString(10000);

  const setResponse = http.post(`http://localhost:9998/keys/${key}`, value);
  check(setResponse, { 'set request suceeded': (r) => r.status == 200 });

  const getResponse = http.get(`http://localhost:9998/keys/${key}`);
  check(getResponse, { 'get request suceeded and value is accurate': (r) => r.status === 200 && r.body === value });
}
