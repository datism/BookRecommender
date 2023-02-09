import React from "react";
import "./book.css";

function Book({ book }) {
  return (
    <div className="book-container">
      <h1 className="book-title">{book['Book-Title']}</h1>
      <img src={book['Image-URL-L']}
           onError={({ currentTarget }) => {
               currentTarget.onerror = null;
               currentTarget.src = process.env.PUBLIC_URL + '/pepe.png';
           }}
      />
    </div>
  );
}

export default Book;
