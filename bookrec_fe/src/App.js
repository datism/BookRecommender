import React, { useState } from "react";
import axios from "axios"
import "./App.css";
import Header from "./components/Header/Header";
import BookList from "./components/BookList/BookList";
import Search from "./components/Search/Search";

function App() {
  const [books, setBooks] = useState([]);

  const recBooks = (user) => {
      console.log(user)
      axios.get("http://localhost:5000/" + user + "/recbooks")
            .then((res) => setBooks(res.data))
  };

  return (
    <div className="App">
      <Header title="Book Recommend" />
      <Search recBooks={recBooks} />
      <BookList books={books} />
    </div>
  );
}

export default App;
