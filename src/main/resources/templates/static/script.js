const uri = '/';

const IS_SELECTED_CLASS = 'isSelected';

let genderUri = 'genders=';
let peerInfluence = 'peerInfluence=';

const genderList = [];
const peerInfluenceList = [];

const peerInfluenceHeader = document.getElementById('peerInfluenceHeader');

configureGenderHeader();

peerInfluenceHeader.querySelectorAll('li').forEach((listElement) => {
  listElement.addEventListener('click', () => {
    const classList = listElement.classList;
    if (classList.contains(IS_SELECTED_CLASS)) {
      deselect(peerInfluenceList, listElement.textContent);
      classList.remove(IS_SELECTED_CLASS);
    } else {
      select(peerInfluenceList, listElement.textContent);
      classList.add(IS_SELECTED_CLASS);
    }
  });
});

function select(list, queryParameter) {
  if (list.includes(queryParameter)) {
    return;
  }
  list.push(queryParameter);
  console.log(list);
}

function deselect(list, queryParameter) {
  if (!list.includes(queryParameter)) {
    return;
  }
  return list.filter((value) => value !== queryParameter);
}

function clearTrailingComma(uri) {
  uri.replace(/,$/, '');
  return uri;
}

function showDropdown(header) {
  const dropdown = header.querySelector('.dropdown-menu');

  const rect = header.getBoundingClientRect();

  dropdown.style.top = `${rect.bottom}px`;
  dropdown.style.left = `${rect.left}px`;
  dropdown.style.display = 'block';
}

function hideDropdown(header) {
  const dropdown = header.querySelector('.dropdown-menu');
  dropdown.style.display = 'none';
}

function configureGenderHeader() {
  const genderHeader = document.getElementById('genderHeader');
  genderHeader.addEventListener('mouseover', (event) => {
    if (event.target !== event.currentTarget) {
      return;
    }
    showDropdown(genderHeader);
  });

  genderHeader.addEventListener('mouseleave', () => {
    hideDropdown(genderHeader);
  });

  genderHeader.querySelectorAll('li').forEach((listElement) => {
    const classList = listElement.classList;
    if (classList.contains('isSelected')) {
      deselect(genderList, listElement.textContent);
      classList.remove(IS_SELECTED_CLASS);
    } else {
      select(genderList, listElement.textContent);
      classList.add(IS_SELECTED_CLASS);
    }
  });
}
