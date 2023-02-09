import React from "react";
import "./search.css";

function Search({ recBooks }) {
    let textInput = React.createRef();

    const handleKeyDown = (event) => {
        if (event.key === 'Enter') {
            recBooks(textInput.current.value)
        }
    }

    return (
        <div className="search">
          <input
            type="search"
            ref={textInput}
            placeholder="Recommend for user"
            onKeyDown={handleKeyDown}
          />
        </div>
    );
}

export default Search;
