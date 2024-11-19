import { showDropdown, hideDropdown, select, deselect } from '../utils.js';
import { IS_SELECTED_CLASS } from '../constants.js';

const genderHeader = document.getElementById('genderHeader');
let genderList = [];

export function configureGenderHeader() {
  genderHeader.addEventListener('mouseover', (event) => {
    if (event.target !== event.currentTarget) {
      return;
    }
    showDropdown(genderHeader);
  });

  genderHeader.addEventListener('mouseleave', () => {
    hideDropdown(genderHeader);
  });

  configureDropDownElements();
}

function configureDropDownElements() {
  genderHeader.querySelectorAll('li').forEach((listElement) => {
    listElement.addEventListener('click', () => {
      const classList = listElement.classList;
      if (classList.contains('isSelected')) {
        genderList = deselect(genderList, listElement.textContent);
        classList.remove(IS_SELECTED_CLASS);
      } else {
        genderList = select(genderList, listElement.textContent);
        classList.add(IS_SELECTED_CLASS);
      }
    });
  });
}

export function getGenderFilters() {
  return genderList.join(',');
}
