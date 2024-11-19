import { showDropdown, hideDropdown, select, deselect } from '../utils.js';
import { IS_SELECTED_CLASS } from '../constants.js';

const peerInfluenceHeader = document.getElementById('peerInfluenceHeader');
let peerInfluenceList = [];

export function configurePeerInfluenceHeader() {
  peerInfluenceHeader.addEventListener('mouseover', (event) => {
    if (event.target !== event.currentTarget) {
      return;
    }
    showDropdown(peerInfluenceHeader);
  });

  peerInfluenceHeader.addEventListener('mouseleave', () => {
    hideDropdown(peerInfluenceHeader);
  });

  configureDropDownElements();
}

function configureDropDownElements() {
  peerInfluenceHeader.querySelectorAll('li').forEach((listElement) => {
    listElement.addEventListener('click', () => {
      const classList = listElement.classList;
      if (classList.contains(IS_SELECTED_CLASS)) {
        peerInfluenceList = deselect(
          peerInfluenceList,
          listElement.textContent
        );
        classList.remove(IS_SELECTED_CLASS);
      } else {
        peerInfluenceList = select(peerInfluenceList, listElement.textContent);
        classList.add(IS_SELECTED_CLASS);
      }
    });
  });
}

export function getPeerInfluenceFilters() {
  return peerInfluenceList.join(',');
}
