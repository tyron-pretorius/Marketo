function TOJSONSAFE(input) {
  if (typeof input !== 'string') {
    return input;
  }

  return input
    .replace(/\\/g, "\\")    // escape backslashes
    .replace(/"/g, "\'")      // escape double quotes
    .replace(/\n/g, "\\n")     // escape new lines
    .replace(/\r/g, "\\r")     // escape carriage returns
    .replace(/\t/g, "\\t");    // escape tabs
}
