import { showDropdown, hideDropdown } from '../utils.js';

const ageGroupHeader = document.getElementById('ageGroupHeader');
const fromInput = ageGroupHeader.querySelector('#fromInput');
const toInput = ageGroupHeader.querySelector('#toInput');

let lowerBoundOfAgeGroup;
let upperBoundOfAgeGroup;

export function configureAgeGroupHeader() {
  ageGroupHeader.addEventListener('mouseover', (event) => {
    if (event.target !== event.currentTarget) {
      return;
    }
    showDropdown(ageGroupHeader);
  });

  ageGroupHeader.addEventListener('mouseleave', () => {
    hideDropdown(ageGroupHeader);
  });

  fromInput.addEventListener(
    'change',
    (event) => (lowerBoundOfAgeGroup = event.target.value)
  );
  toInput.addEventListener(
    'change',
    (event) => (upperBoundOfAgeGroup = event.target.value)
  );
}

export function getAgeGroupFilters() {
  const ageGroup = [];

  lowerBoundOfAgeGroup
    ? ageGroup.push(lowerBoundOfAgeGroup)
    : ageGroup.push('1');

  upperBoundOfAgeGroup
    ? ageGroup.push(upperBoundOfAgeGroup)
    : ageGroup.push('100');

  console.log(ageGroup);

  return ageGroup.join(',');
}
