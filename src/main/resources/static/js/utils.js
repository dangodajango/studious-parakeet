export function showDropdown(header) {
  const dropdown = header.querySelector('.dropdown-menu');

  const rect = header.getBoundingClientRect();

  dropdown.style.top = `${rect.bottom}px`;
  dropdown.style.left = `${rect.left}px`;
  dropdown.style.display = 'block';
}

export function hideDropdown(header) {
  const dropdown = header.querySelector('.dropdown-menu');
  dropdown.style.display = 'none';
}

export function select(list, queryParameter) {
  if (list.includes(queryParameter)) {
    return;
  }
  const temporaryList = [...list];
  temporaryList.push(queryParameter);
  return temporaryList;
}

export function deselect(list, queryParameter) {
  if (!list.includes(queryParameter)) {
    return;
  }
  return list.filter((value) => value !== queryParameter);
}
