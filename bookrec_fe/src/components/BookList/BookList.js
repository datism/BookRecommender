import React from "react";
import Book from "../Book/Book";
import "./bookList.css";

function BookList({ books: books }) {
  return (
    <div className="book-list">
      {books.map((book) => {
        return <Book book={book} />;
      })}
    </div>
  );
}

export default BookList;
